const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET', 'POST'] } });

app.use(cors());
app.use(express.json());

const activeCalls = new Map();

function isoDate(d = new Date()) { return d.toISOString().slice(0, 10); }
function isoWeekKey(d = new Date()) {
  const date = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dayNum = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((date - yearStart) / 86400000) + 1) / 7);
  return String(date.getUTCFullYear()) + '-W' + String(weekNo).padStart(2, '0');
}
function isoMonthKey(d = new Date()) {
  return String(d.getUTCFullYear()) + '-' + String(d.getUTCMonth() + 1).padStart(2, '0');
}

const metrics = { lifetime: { answered: 0, totalDurationSec: 0 }, byDay: new Map(), byWeek: new Map(), byMonth: new Map() };

function getOrInit(map, key) { if (!map.has(key)) map.set(key, { answered: 0, totalDurationSec: 0 }); return map.get(key); }
function isAnsweredCall(call) { return call && call.status === 'ended' && Number(call.duration || 0) >= 5; }
function recordAnsweredCall(call) {
  if (!isAnsweredCall(call)) return;
  const durationSec = Number(call.duration || 0);
  const endedAt = call.endedAt ? new Date(call.endedAt) : new Date();
  metrics.lifetime.answered += 1;
  metrics.lifetime.totalDurationSec += durationSec;
  [getOrInit(metrics.byDay, isoDate(endedAt)), getOrInit(metrics.byWeek, isoWeekKey(endedAt)), getOrInit(metrics.byMonth, isoMonthKey(endedAt))].forEach(b => { b.answered += 1; b.totalDurationSec += durationSec; });
}
function avg(total, count) { return count > 0 ? total / count : 0; }
function getMetricsSnapshot(now = new Date()) {
  const d = metrics.byDay.get(isoDate(now)) || { answered: 0, totalDurationSec: 0 };
  const w = metrics.byWeek.get(isoWeekKey(now)) || { answered: 0, totalDurationSec: 0 };
  const m = metrics.byMonth.get(isoMonthKey(now)) || { answered: 0, totalDurationSec: 0 };
  const l = metrics.lifetime;
  return { callsAnswered: { today: d.answered, thisWeek: w.answered, thisMonth: m.answered, lifetime: l.answered }, avgCallDurationSeconds: { today: avg(d.totalDurationSec, d.answered), thisWeek: avg(w.totalDurationSec, w.answered), thisMonth: avg(m.totalDurationSec, m.answered), lifetime: avg(l.totalDurationSec, l.answered) } };
}

app.post('/vapi-webhook', (req, res) => {
  const msg = req.body && req.body.message;
  if (!msg) return res.sendStatus(200);
  const call = msg.call;
  if (!call) return res.sendStatus(200);
  const callId = call.id;
  if (msg.type === 'status-update') {
    const status = msg.status;
    if (status === 'ringing' || status === 'in-progress') activeCalls.set(callId, { id: callId, status, callerNumber: (call.customer && call.customer.number) || 'Unknown', startedAt: call.startedAt || new Date().toISOString(), assistantId: call.assistantId });
    if (status === 'ended') activeCalls.delete(callId);
    io.emit('live-update', { activeCalls: Array.from(activeCalls.values()), liveCount: activeCalls.size, timestamp: new Date().toISOString() });
  }
  if (msg.type === 'end-of-call-report') recordAnsweredCall(Object.assign({}, call, { status: 'ended' }));
  res.sendStatus(200);
});

app.get('/', (req, res) => res.json({ status: 'ok', service: 'Sarah Live Dashboard', activeCalls: activeCalls.size, uptime: Math.floor(process.uptime()) + 's' }));
app.get('/state', (req, res) => res.json({ activeCalls: Array.from(activeCalls.values()), liveCount: activeCalls.size, timestamp: new Date().toISOString() }));
app.get('/stats', (req, res) => {
  const snap = getMetricsSnapshot();
  const range = req.query.range || 'today';
  const map = { today: { total: snap.callsAnswered.today, avgDuration: snap.avgCallDurationSeconds.today }, week: { total: snap.callsAnswered.thisWeek, avgDuration: snap.avgCallDurationSeconds.thisWeek }, month: { total: snap.callsAnswered.thisMonth, avgDuration: snap.avgCallDurationSeconds.thisMonth }, lifetime: { total: snap.callsAnswered.lifetime, avgDuration: snap.avgCallDurationSeconds.lifetime } };
  res.json({ range, ...(map[range] || map.today), all: map, timestamp: new Date().toISOString() });
});
app.get('/metrics', (req, res) => res.json(getMetricsSnapshot()));

io.on('connection', (socket) => {
  console.log('Dashboard connected:', socket.id);
  socket.emit('live-update', { activeCalls: Array.from(activeCalls.values()), liveCount: activeCalls.size, timestamp: new Date().toISOString() });
  socket.on('disconnect', () => console.log('Dashboard disconnected:', socket.id));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Sarah Live Server running on port ' + PORT));
