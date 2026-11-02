# mcp-socks-proxy

Reach HTTP MCP servers through a SOCKS proxy, from Claude Code or any other MCP client.

It has two modes:

- **`stdio`** — the client launches it as a command. It reads MCP messages on stdin, sends them to the server's
  Streamable HTTP endpoint through the proxy, and writes the replies (JSON or SSE) to stdout. It keeps track of the
  `Mcp-Session-Id` and `MCP-Protocol-Version` headers, opens the server's GET event stream if it has one, and ends
  the session when the client goes away.
- **`serve`** — a local HTTP endpoint that forwards every request to the upstream server through the proxy, with
  bodies streamed through as they are. Use it for clients that only take a URL.

## Install

```sh
cargo install --path .
```

## Use with Claude Code

Start a SOCKS proxy, for example through SSH:

```sh
ssh -N -D 1080 jumphost
```

### stdio (recommended)

```sh
claude mcp add example -- mcp-socks-proxy stdio --proxy socks5h://127.0.0.1:1080 https://mcp.example.com/mcp
```

or in `.mcp.json`:

```json
{
  "mcpServers": {
    "example": {
      "command": "mcp-socks-proxy",
      "args": [
        "stdio",
        "--proxy", "socks5h://127.0.0.1:1080",
        "-H", "Authorization: Bearer ${EXAMPLE_TOKEN}",
        "https://mcp.example.com/mcp"
      ]
    }
  }
}
```

### serve

```sh
mcp-socks-proxy serve --proxy socks5h://127.0.0.1:1080 --listen 127.0.0.1:8931 https://mcp.example.com
claude mcp add --transport http example http://127.0.0.1:8931/mcp
```

The request's path is appended to the upstream URL, and a request for `/` goes to the upstream URL itself, so
`serve https://mcp.example.com/mcp` with `http://127.0.0.1:8931/` works too.

## Options

| Option | |
| --- | --- |
| `-p, --proxy <URL>` | Proxy to use; also read from `MCP_SOCKS_PROXY`. `socks5h://` resolves hostnames through the proxy, `socks5://` resolves them locally. `socks5h://user:pass@host:port` for a proxy that needs a login. |
| `-H, --header "Name: value"` | Header to send upstream, such as `Authorization`. Repeatable. |
| `--connect-timeout <SECONDS>` | How long to wait for a connection through the proxy (default 30). |
| `-l, --listen <ADDR>` | `serve` only: address to listen on (default `127.0.0.1:8931`). |

Logs go to stderr; set `RUST_LOG=mcp_socks_proxy=debug` for more.

## Limitations

- Only the Streamable HTTP transport is supported, not the older HTTP+SSE transport (`/sse` endpoints).
- In `stdio` mode there is no OAuth: pass a token with `-H`. In `serve` mode the client does OAuth itself, but it
  reaches the authorization server directly, not through the proxy, since the server's metadata points at it.
- If the server ends the session, `stdio` mode answers requests with an error; restart the server in the client to
  start a new session.

## Development

```sh
cargo test
```

The end-to-end tests run the binary against a fake MCP server through a small SOCKS5 server.

## License

MIT
