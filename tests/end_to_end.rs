//! Runs the binary against a fake MCP server, reached through a minimal SOCKS5 server.

use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};

const BIN: &str = env!("CARGO_BIN_EXE_mcp-socks-proxy");

/// A SOCKS5 server without authentication that counts the connections it makes.
async fn socks_server() -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let counter = count.clone();
    tokio::spawn(async move {
        loop {
            let (mut client, _) = listener.accept().await.unwrap();
            let counter = counter.clone();
            tokio::spawn(async move {
                let mut head = [0u8; 2];
                client.read_exact(&mut head).await?;
                let mut methods = vec![0u8; head[1] as usize];
                client.read_exact(&mut methods).await?;
                client.write_all(&[5, 0]).await?;
                let mut req = [0u8; 4];
                client.read_exact(&mut req).await?;
                let host = match req[3] {
                    1 => {
                        let mut ip = [0u8; 4];
                        client.read_exact(&mut ip).await?;
                        std::net::Ipv4Addr::from(ip).to_string()
                    }
                    3 => {
                        let len = client.read_u8().await?;
                        let mut name = vec![0u8; len as usize];
                        client.read_exact(&mut name).await?;
                        String::from_utf8(name).unwrap()
                    }
                    other => panic!("unsupported address type {other}"),
                };
                let port = client.read_u16().await?;
                match TcpStream::connect((host.as_str(), port)).await {
                    Ok(mut upstream) => {
                        counter.fetch_add(1, Ordering::SeqCst);
                        client.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
                        tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
                    }
                    Err(_) => client.write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0]).await?,
                }
                std::io::Result::Ok(())
            });
        }
    });
    (addr, count)
}

#[derive(Clone, Default)]
struct Upstream {
    deleted: Arc<Mutex<Vec<String>>>,
}

/// A fake MCP server: answers initialize with JSON and a session id, tools/list with an SSE stream.
async fn mcp_server() -> (SocketAddr, Upstream) {
    let state = Upstream::default();
    let deleted = state.deleted.clone();
    let app = Router::new().route(
        "/mcp",
        post(|headers: HeaderMap, body: String| async move {
            let msg: Value = serde_json::from_str(&body).unwrap();
            let id = msg["id"].clone();
            match msg["method"].as_str().unwrap() {
                "initialize" => (
                    [("mcp-session-id", "session-1"), ("content-type", "application/json")],
                    json!({"jsonrpc": "2.0", "id": id, "result": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "serverInfo": {"name": "fake", "version": "1"},
                    }})
                    .to_string(),
                )
                    .into_response(),
                "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
                "tools/list" => {
                    assert_eq!(headers["mcp-session-id"], "session-1");
                    assert_eq!(headers["mcp-protocol-version"], "2025-06-18");
                    assert_eq!(headers["authorization"], "Bearer secret");
                    let progress = json!({"jsonrpc": "2.0", "method": "notifications/progress", "params": {}});
                    // Pretty-printed, so the event's data spans several lines.
                    let result =
                        serde_json::to_string_pretty(&json!({"jsonrpc": "2.0", "id": id, "result": {"tools": []}}))
                            .unwrap()
                            .lines()
                            .map(|l| format!("data: {l}\n"))
                            .collect::<String>();
                    let body = format!(": hi\n\nevent: message\ndata: {progress}\n\nid: 2\n{result}\n");
                    ([("content-type", "text/event-stream")], body).into_response()
                }
                _ => StatusCode::ACCEPTED.into_response(),
            }
        })
        .get(|| async { StatusCode::METHOD_NOT_ALLOWED })
        .delete(move |headers: HeaderMap| async move {
            deleted.lock().unwrap().push(headers["mcp-session-id"].to_str().unwrap().to_owned());
            StatusCode::OK
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (addr, state)
}

fn spawn(args: &[&str]) -> Child {
    Command::new(BIN)
        .args(args)
        .env_remove("MCP_SOCKS_PROXY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

async fn read_json(lines: &mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>) -> Value {
    let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line()).await.unwrap().unwrap().unwrap();
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn stdio_bridges_through_the_proxy() {
    let (socks, connections) = socks_server().await;
    let (mcp, upstream) = mcp_server().await;
    let proxy = format!("socks5h://{socks}");
    let url = format!("http://{mcp}/mcp");
    let mut child = spawn(&["stdio", "--proxy", &proxy, "-H", "Authorization: Bearer secret", &url]);
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();

    stdin.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}\n").await.unwrap();
    let init = read_json(&mut stdout).await;
    assert_eq!(init["result"]["serverInfo"]["name"], "fake");

    stdin.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n").await.unwrap();
    stdin.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n").await.unwrap();
    assert_eq!(read_json(&mut stdout).await["method"], "notifications/progress");
    assert_eq!(read_json(&mut stdout).await, json!({"jsonrpc": "2.0", "id": 2, "result": {"tools": []}}));

    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(10), child.wait()).await.unwrap().unwrap();
    assert!(status.success());
    assert_eq!(*upstream.deleted.lock().unwrap(), vec!["session-1"]);
    assert!(connections.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn stdio_answers_with_an_error_when_the_server_is_unreachable() {
    let (socks, _) = socks_server().await;
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
    let proxy = format!("socks5h://{socks}");
    let url = format!("http://{closed}/mcp");
    let mut child = spawn(&["stdio", "--proxy", &proxy, &url]);
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();

    stdin.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"x\",\"method\":\"initialize\",\"params\":{}}\n").await.unwrap();
    let reply = read_json(&mut stdout).await;
    assert_eq!(reply["id"], "x");
    assert_eq!(reply["error"]["code"], -32000);
}

#[tokio::test]
async fn serve_forwards_through_the_proxy() {
    let (socks, connections) = socks_server().await;
    let (mcp, _) = mcp_server().await;
    let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
    let listen = format!("127.0.0.1:{port}");
    let proxy = format!("socks5://{socks}");
    let upstream = format!("http://{mcp}");
    let _child = spawn(&["serve", "--listen", &listen, "--proxy", &proxy, &upstream]);

    let body = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}";
    let mut response = None;
    for _ in 0..50 {
        if let Ok(conn) = TcpStream::connect(&listen).await {
            response = Some(http_post(conn, &listen, "/mcp", body).await);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let response = response.expect("the proxy never started listening");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.to_ascii_lowercase().contains("mcp-session-id: session-1"), "{response}");
    assert!(response.contains("\"serverInfo\""), "{response}");
    assert!(connections.load(Ordering::SeqCst) > 0);
}

async fn http_post(mut conn: TcpStream, host: &str, path: &str, body: &[u8]) -> String {
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    conn.write_all(head.as_bytes()).await.unwrap();
    conn.write_all(body).await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(10), conn.read_to_string(&mut response)).await.unwrap().unwrap();
    response
}
