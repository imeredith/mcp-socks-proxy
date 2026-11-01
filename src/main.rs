mod serve;
mod sse;
mod stdio;

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Proxy, Url, redirect};
use tracing_subscriber::EnvFilter;

/// Reach HTTP MCP servers through a SOCKS proxy.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Bridge stdio to a Streamable HTTP MCP server, for clients that launch the server as a command.
    Stdio {
        #[command(flatten)]
        upstream: Upstream,
        /// URL of the MCP endpoint, e.g. https://mcp.example.com/mcp
        url: Url,
    },
    /// Serve a local HTTP endpoint that forwards every request to the upstream server.
    ///
    /// Request paths are appended to the upstream URL, and a request for `/` goes to the upstream URL itself.
    Serve {
        #[command(flatten)]
        upstream: Upstream,
        /// Address to listen on
        #[arg(short, long, default_value = "127.0.0.1:8931")]
        listen: SocketAddr,
        /// Base URL of the upstream server, e.g. https://mcp.example.com
        url: Url,
    },
}

#[derive(Args)]
struct Upstream {
    /// Proxy URL. Use socks5h:// to resolve hostnames through the proxy, socks5:// to resolve them locally.
    #[arg(short, long, env = "MCP_SOCKS_PROXY")]
    proxy: String,
    /// Header to send upstream, as "Name: value". Repeatable.
    #[arg(short = 'H', long = "header", value_name = "HEADER", value_parser = parse_header)]
    headers: Vec<(HeaderName, HeaderValue)>,
    /// Seconds to wait for a connection through the proxy
    #[arg(long, default_value_t = 30, value_name = "SECONDS")]
    connect_timeout: u64,
}

impl Upstream {
    fn client(&self, follow_redirects: bool) -> Result<Client> {
        let proxy = Proxy::all(&self.proxy).with_context(|| format!("invalid proxy URL {:?}", self.proxy))?;
        let redirects = if follow_redirects { redirect::Policy::default() } else { redirect::Policy::none() };
        Client::builder()
            .proxy(proxy)
            .connect_timeout(Duration::from_secs(self.connect_timeout))
            .redirect(redirects)
            .user_agent(concat!("mcp-socks-proxy/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("building HTTP client")
    }

    fn header_map(&self) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in &self.headers {
            map.append(name.clone(), value.clone());
        }
        map
    }
}

fn parse_header(s: &str) -> Result<(HeaderName, HeaderValue), String> {
    let (name, value) = s.split_once(':').ok_or("expected \"Name: value\"")?;
    let name = HeaderName::try_from(name.trim()).map_err(|e| e.to_string())?;
    let value = HeaderValue::try_from(value.trim()).map_err(|e| e.to_string())?;
    Ok((name, value))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr: in stdio mode stdout carries the MCP messages.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("mcp_socks_proxy=info")))
        .init();

    match Cli::parse().command {
        Command::Stdio { upstream, url } => stdio::run(upstream.client(true)?, url, upstream.header_map()).await,
        Command::Serve { upstream, listen, url } => {
            serve::run(upstream.client(false)?, url, listen, upstream.header_map()).await
        }
    }
}

/// Formats an error with its chain of causes, which reqwest keeps out of its `Display`.
pub fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut msg = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        msg.push_str(": ");
        msg.push_str(&cause.to_string());
        source = cause.source();
    }
    msg
}
