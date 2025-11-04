const WebSocket = require('ws');
const http = require('http');
const https = require('https');
const url = require('url');

// Configuration
const PORT = process.env.PORT || 8080;
const CANISTER_URL = process.env.CANISTER_URL || 'https://CANISTER.ic0.app/updates';
const POLL_INTERVAL = 1000; // 1 second

// Track sequence number for commits
let sequenceNumber = 0;

// Create HTTP server
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('AT Protocol WebSocket Server\n');
});

// Create WebSocket server
const wss = new WebSocket.Server({ 
  server,
  verifyClient: (info) => {
    const pathname = url.parse(info.req.url).pathname;
    // Accept connections on /xrpc/com.atproto.sync.subscribeRepos
    return pathname === '/xrpc/com.atproto.sync.subscribeRepos';
  }
});

// Track connected clients
let clients = new Set();

// Handle WebSocket connections
wss.on('connection', (ws, req) => {
  const clientId = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
  console.log(`[${new Date().toISOString()}] Client connected: ${clientId}`);
  
  clients.add(ws);
  
  // Handle client disconnect
  ws.on('close', () => {
    console.log(`[${new Date().toISOString()}] Client disconnected: ${clientId}`);
    clients.delete(ws);
  });
  
  // Handle client errors
  ws.on('error', (error) => {
    console.error(`[${new Date().toISOString()}] Client error (${clientId}):`, error.message);
    clients.delete(ws);
  });
  
  // Handle client messages (if needed for future functionality)
  ws.on('message', (message) => {
    console.log(`[${new Date().toISOString()}] Message from ${clientId}:`, message.toString());
  });
});

// Function to fetch updates from the canister
function fetchCanisterUpdates() {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(CANISTER_URL);
    const protocol = parsedUrl.protocol === 'https:' ? https : http;
    
    protocol.get(CANISTER_URL, (res) => {
      let data = '';
      
      res.on('data', (chunk) => {
        data += chunk;
      });
      
      res.on('end', () => {
        try {
          const updates = JSON.parse(data);
          resolve(updates);
        } catch (error) {
          reject(new Error(`Failed to parse canister response: ${error.message}`));
        }
      });
    }).on('error', (error) => {
      reject(error);
    });
  });
}

// Function to broadcast commit to all connected clients
function broadcastCommit(updates) {
  if (clients.size === 0) {
    return;
  }
  
  sequenceNumber++;
  
  // Transform the updates into AT Protocol commit format
  const commit = {
    "$type": "com.atproto.sync.subscribeRepos#commit",
    "seq": sequenceNumber,
    "ops": updates.map(update => ({
      "action": update.action || "create",
      "path": update.path || "x/y"
    }))
  };
  
  const message = JSON.stringify(commit);
  
  console.log(`[${new Date().toISOString()}] Broadcasting commit seq=${sequenceNumber} to ${clients.size} client(s)`);
  
  // Send to all connected clients
  clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Poll the canister for updates
function startPolling() {
  setInterval(async () => {
    try {
      const updates = await fetchCanisterUpdates();
      
      // Only broadcast if we have updates
      if (updates && Array.isArray(updates) && updates.length > 0) {
        broadcastCommit(updates);
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error fetching canister updates:`, error.message);
    }
  }, POLL_INTERVAL);
  
  console.log(`[${new Date().toISOString()}] Started polling ${CANISTER_URL} every ${POLL_INTERVAL}ms`);
}

// Start the server
server.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] WebSocket server listening on port ${PORT}`);
  console.log(`[${new Date().toISOString()}] Accepting connections on /xrpc/com.atproto.sync.subscribeRepos`);
  startPolling();
});

// Handle graceful shutdown
process.on('SIGTERM', () => {
  console.log(`[${new Date().toISOString()}] SIGTERM received, closing server...`);
  
  // Close all client connections
  clients.forEach((client) => {
    client.close();
  });
  
  server.close(() => {
    console.log(`[${new Date().toISOString()}] Server closed`);
    process.exit(0);
  });
});
