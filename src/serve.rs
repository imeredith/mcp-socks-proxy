//! A local HTTP endpoint that forwards each request to the upstream server through the proxy.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use reqwest::{Client, Url};
use tracing::{debug, info, warn};

use crate::error_chain;

struct Forwarder {
    client: Client,
    upstream: Url,
    headers: HeaderMap,
}

pub async fn run(client: Client, upstream: Url, listen: SocketAddr, headers: HeaderMap) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await.with_context(|| format!("listening on {listen}"))?;
    info!("forwarding http://{} to {upstream}", listener.local_addr()?);
    let app = Router::new().fallback(forward).with_state(Arc::new(Forwarder { client, upstream, headers }));
    axum::serve(listener, app).await.context("serving")
}

async fn forward(State(fwd): State<Arc<Forwarder>>, req: Request) -> Response {
    let target = fwd.target(req.uri());
    let (parts, body) = req.into_parts();
    // MCP requests are small; buffering them lets reqwest send a Content-Length rather than chunk the body.
    let body = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(body) => body,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("mcp-socks-proxy: reading the request: {e}")).into_response();
        }
    };
    let mut headers = parts.headers;
    remove_hop_by_hop(&mut headers);
    headers.remove(header::HOST);
    headers.remove(header::CONTENT_LENGTH);
    for name in fwd.headers.keys() {
        headers.remove(name);
    }
    headers.extend(fwd.headers.clone());

    debug!("{} {target}", parts.method);
    let resp = match fwd.client.request(parts.method, target.clone()).headers(headers).body(body).send().await {
        Ok(resp) => resp,
        Err(e) => {
            let msg = error_chain(&e);
            warn!("{msg}");
            return (StatusCode::BAD_GATEWAY, format!("mcp-socks-proxy: {msg}")).into_response();
        }
    };

    let mut out = Response::builder().status(resp.status());
    let mut resp_headers = resp.headers().clone();
    remove_hop_by_hop(&mut resp_headers);
    *out.headers_mut().unwrap() = resp_headers;
    out.body(Body::from_stream(resp.bytes_stream())).unwrap()
}

impl Forwarder {
    /// Appends the request's path to the upstream URL (`/` stands for the upstream URL itself) and merges the queries.
    fn target(&self, uri: &Uri) -> Url {
        let mut url = self.upstream.clone();
        if uri.path() != "/" {
            let path = format!("{}{}", url.path().trim_end_matches('/'), uri.path());
            url.set_path(&path);
        }
        if let Some(query) = uri.query() {
            let query = match self.upstream.query() {
                Some(base) if !base.is_empty() => format!("{base}&{query}"),
                _ => query.to_owned(),
            };
            url.set_query(Some(&query));
        }
        url
    }
}

fn remove_hop_by_hop(headers: &mut HeaderMap) {
    let listed: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|name| name.trim().to_owned())
        .collect();
    for name in listed {
        headers.remove(name.as_str());
    }
    for name in [
        header::CONNECTION,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ] {
        headers.remove(name);
    }
    headers.remove("keep-alive");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(upstream: &str, uri: &str) -> String {
        let fwd = Forwarder { client: Client::new(), upstream: upstream.parse().unwrap(), headers: HeaderMap::new() };
        fwd.target(&uri.parse().unwrap()).to_string()
    }

    #[test]
    fn maps_paths_onto_the_upstream() {
        assert_eq!(target("https://mcp.example.com", "/mcp"), "https://mcp.example.com/mcp");
        assert_eq!(target("https://mcp.example.com/mcp", "/"), "https://mcp.example.com/mcp");
        assert_eq!(target("https://example.com/api/", "/mcp?a=1"), "https://example.com/api/mcp?a=1");
        assert_eq!(target("https://example.com/mcp?key=k", "/?a=1"), "https://example.com/mcp?key=k&a=1");
        assert_eq!(target("https://example.com/mcp?key=k", "/"), "https://example.com/mcp?key=k");
    }
}
