const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(cors());
app.use(express.json());

const activeCalls = new Map();

app.post('/vapi-webhook', (req, res) => {
  const { message } = req.body;
  if (!message) return res.sendStatus(200);

  const type = message.type;
  const call = message.call;

  if (!call) return res.sendStatus(200);

  const callId = call.id;

  if (type === 'status-update') {
    const status = message.status;

    if (status === 'ringing' || status === 'in-progress') {
      activeCalls.set(callId, {
        id: callId,
        status,
        callerNumber: call.customer?.number || 'Unknown',
        startedAt: call.startedAt || new Date().toISOString(),
        assistantId: call.assistantId
      });
    }

    if (status === 'ended') {
      activeCalls.delete(callId);
    }

    io.emit('live-update', {
      activeCalls: Array.from(activeCalls.values()),
      liveCount: activeCalls.size,
      timestamp: new Date().toISOString()
    });
  }

  res.sendStatus(200);
});

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'Sarah Live Dashboard',
    activeCalls: activeCalls.size,
    uptime: Math.floor(process.uptime()) + 's'
  });
});

app.get('/state', (req, res) => {
  res.json({
    activeCalls: Array.from(activeCalls.values()),
    liveCount: activeCalls.size,
    timestamp: new Date().toISOString()
  });
});

io.on('connection', (socket) => {
  console.log('Dashboard connected:', socket.id);

  socket.emit('live-update', {
    activeCalls: Array.from(activeCalls.values()),
    liveCount: activeCalls.size,
    timestamp: new Date().toISOString()
  });

  socket.on('disconnect', () => {
    console.log('Dashboard disconnected:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Sarah Live Server running on port ${PORT}`);
});
