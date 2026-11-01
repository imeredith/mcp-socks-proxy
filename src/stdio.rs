//! Bridges newline-delimited JSON-RPC on stdin/stdout to an MCP server's Streamable HTTP endpoint.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Client, RequestBuilder, Response, StatusCode, Url};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{error_chain, sse};

const SESSION_ID: &str = "mcp-session-id";
const PROTOCOL_VERSION: &str = "mcp-protocol-version";
const LAST_EVENT_ID: &str = "last-event-id";

pub async fn run(client: Client, url: Url, headers: HeaderMap) -> Result<()> {
    let (out, mut out_rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(line) = out_rx.recv().await {
            let written = async {
                stdout.write_all(line.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await
            };
            if let Err(e) = written.await {
                warn!("writing to stdout: {e}");
                return;
            }
        }
    });

    info!("bridging stdio to {url}");
    let bridge = Arc::new(Bridge {
        client,
        url,
        headers,
        session_id: Mutex::default(),
        protocol_version: Mutex::default(),
        get_stream_started: AtomicBool::new(false),
        out,
    });

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(line) {
            Ok(msg) => msg,
            Err(e) => {
                warn!("ignoring a line on stdin that isn't JSON: {e}");
                continue;
            }
        };
        let ids = request_ids(&msg);
        if ids.is_empty() {
            // Notifications and responses are acknowledged straight away, so send them in order: a server
            // may insist on seeing notifications/initialized before the requests that follow it.
            let initialized = msg.get("method").and_then(Value::as_str) == Some("notifications/initialized");
            match bridge.post(&msg).await {
                Ok(resp) => {
                    tokio::spawn(bridge.clone().forward(resp, ids));
                }
                Err(e) => warn!("sending to {}: {}", bridge.url, error_chain(&e)),
            }
            if initialized {
                bridge.clone().start_get_stream();
            }
        } else {
            // Requests can take a while to answer, so don't hold up the ones behind them.
            let bridge = bridge.clone();
            tokio::spawn(async move {
                match bridge.post(&msg).await {
                    Ok(resp) => bridge.forward(resp, ids).await,
                    Err(e) => bridge.fail(&ids, &error_chain(&e)),
                }
            });
        }
    }

    // stdin closed: the client has gone away.
    bridge.end_session().await;
    Ok(())
}

struct Bridge {
    client: Client,
    url: Url,
    headers: HeaderMap,
    session_id: Mutex<Option<HeaderValue>>,
    protocol_version: Mutex<Option<HeaderValue>>,
    get_stream_started: AtomicBool,
    out: mpsc::UnboundedSender<String>,
}

impl Bridge {
    fn request(&self, method: reqwest::Method) -> RequestBuilder {
        let mut req = self.client.request(method, self.url.clone()).headers(self.headers.clone());
        if let Some(id) = self.session_id.lock().unwrap().clone() {
            req = req.header(SESSION_ID, id);
        }
        if let Some(version) = self.protocol_version.lock().unwrap().clone() {
            req = req.header(PROTOCOL_VERSION, version);
        }
        req
    }

    async fn post(&self, msg: &Value) -> reqwest::Result<Response> {
        self.request(reqwest::Method::POST)
            .header(ACCEPT, "application/json, text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .body(msg.to_string())
            .send()
            .await
    }

    /// Passes the server's reply to a POST on to stdout, and answers with an error any of `ids` it leaves unanswered.
    async fn forward(self: Arc<Self>, resp: Response, mut ids: Vec<Value>) {
        if let Some(id) = resp.headers().get(SESSION_ID) {
            *self.session_id.lock().unwrap() = Some(id.clone());
        }
        let status = resp.status();
        if status == StatusCode::NOT_FOUND && self.session_id.lock().unwrap().take().is_some() {
            self.fail(&ids, "the MCP server ended the session (HTTP 404); reconnect to start a new one");
            return;
        }
        let content_type = content_type(&resp);
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            // Some servers report errors as JSON-RPC messages with a 4xx status.
            match serde_json::from_str::<Value>(&body) {
                Ok(msg) if content_type == "application/json" && is_jsonrpc(&msg) => self.emit(msg, &mut ids),
                _ => warn!("HTTP {status} from {}: {}", self.url, truncate(&body, 500)),
            }
            self.fail(&ids, &format!("HTTP {status} from {}: {}", self.url, truncate(&body, 500)));
            return;
        }
        if status == StatusCode::ACCEPTED || status == StatusCode::NO_CONTENT {
            self.fail(&ids, "the MCP server accepted the request without answering it");
            return;
        }
        match content_type.as_str() {
            "text/event-stream" => {
                let mut last_event_id = None;
                if let Err(e) = self.pump_events(resp, &mut ids, &mut last_event_id).await {
                    warn!("event stream from {} broke off: {}", self.url, error_chain(&e));
                }
            }
            "application/json" => match resp.text().await {
                Ok(body) => match serde_json::from_str(&body) {
                    Ok(msg) => self.emit(msg, &mut ids),
                    Err(e) => warn!("the server's reply isn't JSON ({e}): {}", truncate(&body, 500)),
                },
                Err(e) => warn!("reading the reply from {}: {}", self.url, error_chain(&e)),
            },
            _ if ids.is_empty() => {}
            other => warn!("unexpected content type {other:?} from {}", self.url),
        }
        self.fail(&ids, "the MCP server didn't answer the request");
    }

    /// Writes each message event of an SSE response to stdout. Returns when the stream ends.
    async fn pump_events(
        &self,
        resp: Response,
        ids: &mut Vec<Value>,
        last_event_id: &mut Option<String>,
    ) -> reqwest::Result<()> {
        let mut parser = sse::Parser::default();
        parser.last_event_id = last_event_id.take();
        let mut body = resp.bytes_stream();
        let result = loop {
            let chunk = match body.next().await {
                Some(Ok(chunk)) => chunk,
                Some(Err(e)) => break Err(e),
                None => break Ok(()),
            };
            for event in parser.push(&chunk) {
                if event.event != "message" || event.data.is_empty() {
                    continue;
                }
                match serde_json::from_str(&event.data) {
                    Ok(msg) => self.emit(msg, ids),
                    Err(e) => warn!("ignoring an event that isn't JSON ({e}): {}", truncate(&event.data, 500)),
                }
            }
        };
        *last_event_id = parser.last_event_id;
        result
    }

    /// Writes a message from the server to stdout, crossing off any of `ids` it answers.
    fn emit(&self, msg: Value, ids: &mut Vec<Value>) {
        for m in messages(&msg) {
            if m.get("method").is_none()
                && let Some(id) = m.get("id")
            {
                ids.retain(|pending| pending != id);
            }
            // Later requests must carry the protocol version the server chose in its initialize result.
            let result = m.get("result");
            if let (Some(version), Some(_)) = (
                result.and_then(|r| r.get("protocolVersion")).and_then(Value::as_str),
                result.and_then(|r| r.get("serverInfo")),
            ) && let Ok(version) = HeaderValue::from_str(version)
            {
                *self.protocol_version.lock().unwrap() = Some(version);
            }
        }
        let _ = self.out.send(msg.to_string());
    }

    /// Answers each of `ids` with a JSON-RPC error, so the client isn't left waiting.
    fn fail(&self, ids: &[Value], message: &str) {
        for id in ids {
            warn!("answering request {id} with an error: {message}");
            let error = json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32000, "message": format!("mcp-socks-proxy: {message}") },
            });
            let _ = self.out.send(error.to_string());
        }
    }

    /// Opens the optional GET stream the server can use to send requests and notifications of its own.
    fn start_get_stream(self: Arc<Self>) {
        if self.get_stream_started.swap(true, Ordering::SeqCst) {
            return;
        }
        tokio::spawn(async move {
            let mut last_event_id = None;
            let mut backoff = Duration::from_secs(1);
            loop {
                let mut req = self.request(reqwest::Method::GET).header(ACCEPT, "text/event-stream");
                if let Some(id) = &last_event_id {
                    req = req.header(LAST_EVENT_ID, id);
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() && content_type(&resp) == "text/event-stream" => {
                        debug!("opened the server's event stream");
                        backoff = Duration::from_secs(1);
                        if let Err(e) = self.pump_events(resp, &mut Vec::new(), &mut last_event_id).await {
                            debug!("the server's event stream broke off: {}", error_chain(&e));
                        }
                    }
                    Ok(resp) if resp.status() == StatusCode::METHOD_NOT_ALLOWED => {
                        debug!("the server doesn't offer an event stream");
                        return;
                    }
                    Ok(resp) if resp.status().is_client_error() => {
                        warn!("the server refused its event stream: HTTP {}", resp.status());
                        return;
                    }
                    Ok(resp) => warn!("opening the server's event stream: HTTP {}", resp.status()),
                    Err(e) => warn!("opening the server's event stream: {}", error_chain(&e)),
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        });
    }

    async fn end_session(&self) {
        if self.session_id.lock().unwrap().is_none() {
            return;
        }
        let req = self.request(reqwest::Method::DELETE).timeout(Duration::from_secs(5));
        if let Err(e) = req.send().await {
            debug!("ending the session: {}", error_chain(&e));
        }
    }
}

/// The ids of the requests in a message or batch, which each expect a response.
fn request_ids(msg: &Value) -> Vec<Value> {
    messages(msg).filter(|m| m.get("method").is_some()).filter_map(|m| m.get("id").cloned()).collect()
}

fn messages(msg: &Value) -> impl Iterator<Item = &Value> {
    match msg {
        Value::Array(batch) => batch.iter(),
        single => std::slice::from_ref(single).iter(),
    }
}

fn is_jsonrpc(msg: &Value) -> bool {
    messages(msg).all(|m| m.get("jsonrpc").is_some())
}

fn content_type(resp: &Response) -> String {
    resp.headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_default()
}

fn truncate(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_request_ids() {
        assert_eq!(request_ids(&json!({"jsonrpc": "2.0", "id": 1, "method": "ping"})), vec![json!(1)]);
        assert!(request_ids(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"})).is_empty());
        assert!(request_ids(&json!({"jsonrpc": "2.0", "id": 1, "result": {}})).is_empty());
        let batch = json!([
            {"jsonrpc": "2.0", "id": "a", "method": "ping"},
            {"jsonrpc": "2.0", "method": "notifications/progress"},
        ]);
        assert_eq!(request_ids(&batch), vec![json!("a")]);
    }
}
