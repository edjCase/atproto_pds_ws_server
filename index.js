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
  console.log(`[${new Date().toISOString()}] HTTP request: ${req.method} ${req.url}`);
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('AT Protocol WebSocket Server\n');
});

// Create WebSocket server
const wss = new WebSocket.Server({
  server,
});

// Handle WebSocket connections
wss.on('connection', (ws, req) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const clientId = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
  const initialCursor = parsedUrl.searchParams.get('cursor');

  console.log(`[${new Date().toISOString()}] ===== NEW CONNECTION =====`);
  console.log(`  Client: ${clientId}`);
  console.log(`  URL: ${req.url}`);
  console.log(`  Initial cursor: ${initialCursor}`);
  console.log(`  Headers:`, req.headers);
  console.log(`  WS ready state: ${ws.readyState}`);

  let lastSeq = initialCursor ? parseInt(initialCursor) : 0;
  let pollInterval = null;
  let isActive = true;
  let messagesSent = 0;
  let pollCount = 0;

  // Function to fetch messages from the API
  async function fetchMessages() {
    if (!isActive || ws.readyState !== WebSocket.OPEN) {
      console.log(`[${new Date().toISOString()}] Skipping poll for ${clientId} - isActive: ${isActive}, readyState: ${ws.readyState}`);
      return;
    }

    pollCount++;
    const apiUrl = `${API_URL}?sinceSeq=${lastSeq}`;
    console.log(`[${new Date().toISOString()}] Poll #${pollCount} for ${clientId}: ${apiUrl}`);

    return new Promise((resolve, reject) => {
      const startTime = Date.now();
      https.get(apiUrl, (res) => {
        let data = '';

        res.on('data', (chunk) => {
          data += chunk;
        });

        res.on('end', () => {
          const duration = Date.now() - startTime;
          console.log(`[${new Date().toISOString()}] API response for ${clientId}: status=${res.statusCode}, duration=${duration}ms, size=${data.length} bytes`);

          try {
            const contentType = res.headers['content-type'] || '';

            // Handle 400 errors with JSON response
            if (res.statusCode === 400 && contentType.includes('application/json')) {
              const errorData = JSON.parse(data);
              console.log(`[${new Date().toISOString()}] API error 400 for ${clientId}:`, errorData);
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(errorData));
                console.log(`[${new Date().toISOString()}] Sent error to ${clientId}`);
              }
              resolve();
              return;
            }

            // Handle other error status codes
            if (res.statusCode !== 200) {
              console.error(`[${new Date().toISOString()}] API error for ${clientId}: status ${res.statusCode}, body: ${data.substring(0, 200)}`);
              reject(new Error(`API returned status ${res.statusCode}`));
              return;
            }

            const response = JSON.parse(data);
            console.log(`[${new Date().toISOString()}] API response parsed for ${clientId}: ${response.messages?.length || 0} messages`);
            if (response.messages?.length > 0) {
              console.log(`  First message seq: ${response.messages[0].seq}`);
              console.log(`  Last message seq: ${response.messages[response.messages.length - 1].seq}`);
            }
            resolve(response);
          } catch (error) {
            console.error(`[${new Date().toISOString()}] Parse error for ${clientId}:`, error.message, 'Data:', data.substring(0, 200));
            reject(new Error(`Failed to parse API response: ${error.message}`));
          }
        });
      }).on('error', (error) => {
        console.error(`[${new Date().toISOString()}] HTTPS request error for ${clientId}:`, error.message);
        reject(error);
      });
    });
  }

  // Function to poll and send messages
  async function pollAndSend() {
    try {
      const response = await fetchMessages();

      if (!response || !response.messages || !Array.isArray(response.messages)) {
        console.log(`[${new Date().toISOString()}] No messages in response for ${clientId}`);
        return;
      }

      // Send each message individually
      for (const message of response.messages) {
        if (ws.readyState === WebSocket.OPEN) {
          const messageStr = JSON.stringify(message);
          console.log(`[${new Date().toISOString()}] Sending message to ${clientId}: seq=${message.seq}, size=${messageStr.length} bytes`);
          ws.send(messageStr);
          messagesSent++;

          // Update lastSeq to the highest seq value
          if (message.seq && message.seq > lastSeq) {
            lastSeq = message.seq;
          }
        } else {
          console.log(`[${new Date().toISOString()}] Cannot send to ${clientId} - readyState: ${ws.readyState}`);
          break;
        }
      }

      if (response.messages.length > 0) {
        console.log(`[${new Date().toISOString()}] Completed sending ${response.messages.length} message(s) to ${clientId}, lastSeq=${lastSeq}, totalSent=${messagesSent}`);
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error polling for ${clientId}:`, error.message);
      console.error(`  Stack:`, error.stack);
      // Close the WebSocket on unhandled errors
      if (ws.readyState === WebSocket.OPEN) {
        console.log(`[${new Date().toISOString()}] Closing connection for ${clientId} due to error`);
        ws.close(1011, 'Internal error');
      }
    }
  }

  // Start polling for this client
  pollInterval = setInterval(pollAndSend, POLL_INTERVAL);
  // Do an immediate poll on connection
  console.log(`[${new Date().toISOString()}] Starting immediate poll for ${clientId}`);
  pollAndSend();

  // Handle client disconnect
  ws.on('close', (code, reason) => {
    console.log(`[${new Date().toISOString()}] ===== CLIENT DISCONNECTED =====`);
    console.log(`  Client: ${clientId}`);
    console.log(`  Code: ${code}`);
    console.log(`  Reason: ${reason || 'none'}`);
    console.log(`  Total polls: ${pollCount}`);
    console.log(`  Total messages sent: ${messagesSent}`);
    console.log(`  Final seq: ${lastSeq}`);
    isActive = false;
    if (pollInterval) {
      clearInterval(pollInterval);
    }
  });

  // Handle client errors
  ws.on('error', (error) => {
    console.error(`[${new Date().toISOString()}] ===== CLIENT ERROR =====`);
    console.error(`  Client: ${clientId}`);
    console.error(`  Error:`, error.message);
    console.error(`  Stack:`, error.stack);
    isActive = false;
    if (pollInterval) {
      clearInterval(pollInterval);
    }
  });

  // Handle client messages (if needed for future functionality)
  ws.on('message', (message) => {
    console.log(`[${new Date().toISOString()}] Message received from ${clientId}: ${message.toString().substring(0, 200)}`);
  });
});

// Log WebSocket server errors
wss.on('error', (error) => {
  console.error(`[${new Date().toISOString()}] WebSocket server error:`, error);
});

// Start the server
server.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] =============================`);
  console.log(`[${new Date().toISOString()}] WebSocket server listening on port ${PORT}`);
  console.log(`[${new Date().toISOString()}] API endpoint: ${API_URL}`);
  console.log(`[${new Date().toISOString()}] Poll interval: ${POLL_INTERVAL}ms`);
  console.log(`[${new Date().toISOString()}] =============================`);
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