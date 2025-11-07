const WebSocket = require('ws');
const http = require('http');
const https = require('https');
const url = require('url');

// Configuration
const PORT = process.env.PORT || 8080;
const API_URL = 'https://pds.edjcase.com/api/getRepoMessages';
const POLL_INTERVAL = 30000; // 30 seconds

// Create HTTP server
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('AT Protocol WebSocket Server\n');
});

// Create WebSocket server
const wss = new WebSocket.Server({
  server,
});

// Handle WebSocket connections
wss.on('connection', (ws, req) => {
  const clientId = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
  console.log(`[${new Date().toISOString()}] Client connected: ${clientId}`);

  // Track the last sequence number for this client
  let lastSeq = parsedUrl.query.cursor ? parseInt(parsedUrl.query.cursor) : 0;
  let pollInterval = null;
  let isActive = true;

  // Function to fetch messages from the API
  async function fetchMessages() {
    if (!isActive || ws.readyState !== WebSocket.OPEN) {
      return;
    }

    const apiUrl = `${API_URL}?sinceSeq=${lastSeq}`;

    return new Promise((resolve, reject) => {
      https.get(apiUrl, (res) => {
        let data = '';

        res.on('data', (chunk) => {
          data += chunk;
        });

        res.on('end', () => {
          try {
            const contentType = res.headers['content-type'] || '';

            // Handle 400 errors with JSON response
            if (res.statusCode === 400 && contentType.includes('application/json')) {
              const errorData = JSON.parse(data);
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(errorData));
              }
              resolve();
              return;
            }

            // Handle other error status codes
            if (res.statusCode !== 200) {
              reject(new Error(`API returned status ${res.statusCode}`));
              return;
            }

            const response = JSON.parse(data);
            resolve(response);
          } catch (error) {
            reject(new Error(`Failed to parse API response: ${error.message}`));
          }
        });
      }).on('error', (error) => {
        reject(error);
      });
    });
  }

  // Function to poll and send messages
  async function pollAndSend() {
    try {
      const response = await fetchMessages();

      if (!response || !response.messages || !Array.isArray(response.messages)) {
        return;
      }

      // Send each message individually
      for (const message of response.messages) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify(message));

          // Update lastSeq to the highest seq value
          if (message.seq && message.seq > lastSeq) {
            lastSeq = message.seq;
          }
        }
      }

      if (response.messages.length > 0) {
        console.log(`[${new Date().toISOString()}] Sent ${response.messages.length} message(s) to ${clientId}, lastSeq=${lastSeq}`);
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error polling for ${clientId}:`, error.message);
      // Close the WebSocket on unhandled errors
      if (ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    }
  }

  // Start polling for this client
  pollInterval = setInterval(pollAndSend, POLL_INTERVAL);
  // Do an immediate poll on connection
  pollAndSend();

  // Handle client disconnect
  ws.on('close', () => {
    console.log(`[${new Date().toISOString()}] Client disconnected: ${clientId}`);
    isActive = false;
    if (pollInterval) {
      clearInterval(pollInterval);
    }
  });

  // Handle client errors
  ws.on('error', (error) => {
    console.error(`[${new Date().toISOString()}] Client error (${clientId}):`, error.message);
    isActive = false;
    if (pollInterval) {
      clearInterval(pollInterval);
    }
  });

  // Handle client messages (if needed for future functionality)
  ws.on('message', (message) => {
    console.log(`[${new Date().toISOString()}] Message from ${clientId}:`, message.toString());
  });
});

// Start the server
server.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] WebSocket server listening on port ${PORT}`);
  console.log(`[${new Date().toISOString()}] Accepting connections`);
  console.log(`[${new Date().toISOString()}] Will poll ${API_URL} every ${POLL_INTERVAL}ms per client`);
});

// Handle graceful shutdown
process.on('SIGTERM', () => {
  console.log(`[${new Date().toISOString()}] SIGTERM received, closing server...`);

  wss.clients.forEach((client) => {
    client.close();
  });

  server.close(() => {
    console.log(`[${new Date().toISOString()}] Server closed`);
    process.exit(0);
  });
});
