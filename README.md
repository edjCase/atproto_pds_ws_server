# atproto_pds_ws_server

A WebSocket server that accepts AT Protocol `com.atproto.sync.subscribeRepos` connections and polls the IC PDS canister for updates, broadcasting them to connected clients.

## Features

-   WebSocket server accepting connections on `/xrpc/com.atproto.sync.subscribeRepos`
-   Polls a configurable canister endpoint every 1 second
-   Broadcasts updates as AT Protocol commits with incremental sequence numbers
-   Handles client connections and disconnections gracefully
-   Comprehensive activity logging
-   Configurable via environment variables
-   Ready for Fly.io deployment

## Installation

```bash
npm install
```

## Usage

### Local Development

Start the server:

```bash
npm start
```

The server will listen on port 8080 by default.

### Configuration

Configure the server using environment variables:

-   `DOMAIN` - Domain to poll for updates (Required)
-   `PORT` - Server port (Optional, default: 8080)

Example:

```bash
PORT=8080 DOMAIN=my-domain.com npm start
```

### Connecting Clients

Connect using any WebSocket client to:

```
ws://localhost:8080/xrpc/com.atproto.sync.subscribeRepos
```

Clients will receive updates in AT Protocol format:

```json
{
    "$type": "com.atproto.sync.subscribeRepos#commit",
    "seq": 1,
    "ops": [
        {
            "action": "create",
            "path": "app.bsky.feed.post/abc123"
        }
    ]
}
```

## License

MIT
