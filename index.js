const WebSocket = require('ws');
const http = require('http');
const https = require('https');

// Configuration
const PORT = process.env.PORT || 8080;
const DOMAIN = process.env.DOMAIN;
if (!DOMAIN) {
  console.error('Error: DOMAIN environment variable is not set.');
  process.exit(1);
}
const API_URL = `https://${DOMAIN}/api/getRepoMessages`;
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
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const clientId = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
  const initialCursor = parsedUrl.searchParams.get('cursor');

  console.log(`[${new Date().toISOString()}] Client connected: ${clientId}, cursor: ${initialCursor}, url: ${req.url}`);

  let lastSeq = initialCursor ? parseInt(initialCursor) : 0;
  let pollInterval = null;
  let isActive = true;
  let messagesSent = 0;

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
              console.error(`[${new Date().toISOString()}] API error ${res.statusCode} for ${clientId}`);
              reject(new Error(`API returned status ${res.statusCode}`));
              return;
            }

            const response = JSON.parse(data);
            resolve(response);
          } catch (error) {
            console.error(`[${new Date().toISOString()}] Parse error for ${clientId}: ${error.message}`);
            reject(new Error(`Failed to parse API response: ${error.message}`));
          }
        });
      }).on('error', (error) => {
        console.error(`[${new Date().toISOString()}] Request error for ${clientId}: ${error.message}`);
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

      // Send each message individually as binary
      for (let i = 0; i < response.messages.length; i++) {
        if (ws.readyState === WebSocket.OPEN) {
          const base64Message = response.messages[i];
          const binaryMessage = Buffer.from(base64Message, 'base64');
          console.log(`[${new Date().toISOString()}] Sending message #${messagesSent + 1} to ${clientId}, seq: ${lastSeq + 1}, base64: ${base64Message}`);
          ws.send(binaryMessage);
          messagesSent++;
          lastSeq++;
        } else {
          console.log(`[${new Date().toISOString()}] WebSocket closed during message send for ${clientId}, stopping at message ${i + 1}/${response.messages.length}`);
          break;
        }
      }

      if (response.messages.length > 0) {
        console.log(`[${new Date().toISOString()}] Sent ${response.messages.length} messages to ${clientId}`);
      }
    } catch (error) {
      console.error(`[${new Date().toISOString()}] Error for ${clientId}: ${error.message}`);
      if (ws.readyState === WebSocket.OPEN) {
        ws.close(1011, 'Internal error');
      }
    }
  }

  // Start polling for this client
  pollInterval = setInterval(pollAndSend, POLL_INTERVAL);
  pollAndSend();

  // Handle client disconnect
  ws.on('close', (code, reason) => {
    const reasonText = reason ? reason.toString() : 'No reason provided';
    console.log(`[${new Date().toISOString()}] Client disconnected: ${clientId}, code: ${code}, reason: "${reasonText}", sent ${messagesSent} messages, final cursor: ${lastSeq}`);
    isActive = false;
    if (pollInterval) {
      clearInterval(pollInterval);
    }
  });

  // Handle client errors
  ws.on('error', (error) => {
    console.error(`[${new Date().toISOString()}] Client error: ${clientId} - ${error.message}`);
    isActive = false;
    if (pollInterval) {
      clearInterval(pollInterval);
    }
  });

  // Handle client messages (if needed for future functionality)
  ws.on('message', (message) => {
    console.log(`[${new Date().toISOString()}] Message from ${clientId}: ${message.toString().substring(0, 100)}`);
  });
});

// Log WebSocket server errors
wss.on('error', (error) => {
  console.error(`[${new Date().toISOString()}] WebSocket server error:`, error);
});

// Start the server
server.listen(PORT, () => {
  console.log(`[${new Date().toISOString()}] WebSocket server listening on port ${PORT}`);
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