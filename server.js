/**
 * Sarah Live Dashboard — server.js
 * Railway + Redis + Socket.IO
 * Tracks VAPI calls only. No Make.com dependency.
 */

const express    = require("express");
const http       = require("http");
const { Server } = require("socket.io");
const cors       = require("cors");
const { createClient } = require("redis");

const PORT           = process.env.PORT || 3000;
const REDIS_URL      = process.env.REDIS_URL;
const WEBHOOK_SECRET = process.env.VAPI_WEBHOOK_SECRET;

// ── APP / SOCKET.IO ───────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

app.use(cors());
app.use(express.json({ limit: "2mb" }));

// ── REDIS ─────────────────────────────────────────────────────
if (!REDIS_URL) {
  console.warn("WARN: REDIS_URL is not set. This service will not function correctly.");
}

const redis = createClient({ url: REDIS_URL });
redis.on("error", (err) => console.error("Redis error:", err));

// ── HELPERS ───────────────────────────────────────────────────
function isoDate(d = new Date()) {
  return d.toISOString().slice(0, 10);
}

function isoWeekKey(d = new Date()) {
  const date   = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dayNum = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const weekNo    = Math.ceil((((date - yearStart) / 86400000) + 1) / 7);
  return `${date.getUTCFullYear()}-W${String(weekNo).padStart(2, "0")}`;
}

function isoMonthKey(d = new Date()) {
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
}

function safeJsonParse(str) {
  try { return JSON.parse(str); } catch { return null; }
}

// ── ACTIVE CALLS ──────────────────────────────────────────────
async function setActiveCall(callId, data) {
  await redis.hSet("activeCalls", callId, JSON.stringify(data));
}

async function removeActiveCall(callId) {
  await redis.hDel("activeCalls", callId);
}

async function getActiveCalls() {
  const all = await redis.hGetAll("activeCalls");
  return Object.values(all).map(safeJsonParse).filter(Boolean);
}

// ── METRICS ───────────────────────────────────────────────────
async function recordAnsweredCall(call) {
  const duration = Number(call?.duration ?? 0);
  if (duration < 1) return; // count all calls over 1 second

  const endedAt = call?.endedAt ? new Date(call.endedAt) : new Date();
  const keys = [
    "metrics:lifetime",
    `metrics:day:${isoDate(endedAt)}`,
    `metrics:week:${isoWeekKey(endedAt)}`,
    `metrics:month:${isoMonthKey(endedAt)}`,
  ];

  const multi = redis.multi();
  for (const key of keys) {
    multi.hIncrBy(key, "answered", 1);
    multi.hIncrByFloat(key, "totalDuration", duration);
  }
  await multi.exec();
}

async function getMetricsSnapshot(now = new Date()) {
  const keyMap = {
    today:     `metrics:day:${isoDate(now)}`,
    thisWeek:  `metrics:week:${isoWeekKey(now)}`,
    thisMonth: `metrics:month:${isoMonthKey(now)}`,
    lifetime:  "metrics:lifetime",
  };

  const multi = redis.multi();
  for (const key of Object.values(keyMap)) multi.hGetAll(key);
  const rows = await multi.exec();

  const datas = rows.map((r) => (Array.isArray(r) ? r[1] : r) || {});
  const results = {};
  Object.keys(keyMap).forEach((label, i) => {
    const d        = datas[i] || {};
    const answered = parseInt(d.answered || "0", 10);
    const total    = parseFloat(d.totalDuration || "0");
    results[label] = {
      answered,
      avgDuration: answered > 0 ? total / answered : 0,
    };
  });

  return {
    callsAnswered: {
      today:     results.today.answered,
      thisWeek:  results.thisWeek.answered,
      thisMonth: results.thisMonth.answered,
      lifetime:  results.lifetime.answered,
    },
    avgCallDurationSeconds: {
      today:     results.today.avgDuration,
      thisWeek:  results.thisWeek.avgDuration,
      thisMonth: results.thisMonth.avgDuration,
      lifetime:  results.lifetime.avgDuration,
    },
  };
}

// ── BROADCAST ─────────────────────────────────────────────────
async function broadcastLiveUpdate() {
  const activeCalls = await getActiveCalls();
  io.emit("live-update", {
    activeCalls,
    liveCount:  activeCalls.length,
    timestamp:  new Date().toISOString(),
  });
}

// ── PURGE STALE CALLS (every 10 min) ──────────────────────────
setInterval(async () => {
  try {
    const all    = await redis.hGetAll("activeCalls");
    const cutoff = Date.now() - 2 * 60 * 60 * 1000;
    const multi  = redis.multi();
    let purged   = 0;
    for (const [id, val] of Object.entries(all)) {
      const call = safeJsonParse(val);
      if (!call?.startedAt) continue;
      if (new Date(call.startedAt).getTime() < cutoff) {
        multi.hDel("activeCalls", id);
        purged += 1;
      }
    }
    if (purged > 0) {
      await multi.exec();
      console.log(`Purged stale calls: ${purged}`);
      await broadcastLiveUpdate();
    }
  } catch (e) {
    console.error("Purge job error:", e);
  }
}, 10 * 60 * 1000);

// ── VAPI WEBHOOK HANDLER ──────────────────────────────────────
async function handleVapiWebhook(body) {
  const msg = body?.message;
  if (!msg) return;
  const call   = msg.call;
  if (!call?.id) return;
  const callId = call.id;

  if (msg.type === "status-update") {
    const status = msg.status;
    if (status === "ringing" || status === "in-progress") {
      await setActiveCall(callId, {
        id:           callId,
        status,
        callerNumber: call?.customer?.number || "Unknown",
        startedAt:    call.startedAt || new Date().toISOString(),
        assistantId:  call.assistantId,
      });
    }
    if (status === "ended") {
      await removeActiveCall(callId);
      // Record here too in case end-of-call-report is not sent
      await recordAnsweredCall(call);
    }
    await broadcastLiveUpdate();
  }

  if (msg.type === "end-of-call-report") {
    await removeActiveCall(callId);
    await recordAnsweredCall(call);
    await broadcastLiveUpdate();
  }

  // Some VAPI plans send call-ended instead of end-of-call-report
  if (msg.type === "call-ended") {
    await removeActiveCall(callId);
    await recordAnsweredCall(call);
    await broadcastLiveUpdate();
  }
}

// ── WEBHOOK ROUTES ────────────────────────────────────────────
// Unsecured (only works if VAPI_WEBHOOK_SECRET is not set)
app.post("/vapi-webhook", (req, res) => {
  if (WEBHOOK_SECRET) return res.sendStatus(403);
  res.sendStatus(200);
  handleVapiWebhook(req.body).catch((e) => console.error("Webhook error:", e));
});

// Secured — set Vapi Server URL to:
// https://<your-domain>/vapi-webhook/<VAPI_WEBHOOK_SECRET>
app.post("/vapi-webhook/:secret", (req, res) => {
  if (WEBHOOK_SECRET && req.params.secret !== WEBHOOK_SECRET) {
    return res.sendStatus(401);
  }
  res.sendStatus(200);
  handleVapiWebhook(req.body).catch((e) => console.error("Webhook error:", e));
});

// ── REST ROUTES ───────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({ status: "ok", service: "Sarah Live Dashboard", uptime: `${Math.floor(process.uptime())}s` });
});

app.get("/health", async (req, res) => {
  try {
    await redis.ping();
    res.json({ status: "ok", redis: "connected" });
  } catch (e) {
    res.status(503).json({ status: "error", redis: "disconnected" });
  }
});

app.get("/state", async (req, res) => {
  const activeCalls = await getActiveCalls();
  res.json({ activeCalls, liveCount: activeCalls.length, timestamp: new Date().toISOString() });
});

app.get("/metrics", async (req, res) => {
  res.json(await getMetricsSnapshot());
});

app.get("/stats", async (req, res) => {
  const snap  = await getMetricsSnapshot();
  const range = req.query.range || "today";
  const map   = {
    today:    { total: snap.callsAnswered.today,     avgDuration: snap.avgCallDurationSeconds.today },
    week:     { total: snap.callsAnswered.thisWeek,  avgDuration: snap.avgCallDurationSeconds.thisWeek },
    month:    { total: snap.callsAnswered.thisMonth, avgDuration: snap.avgCallDurationSeconds.thisMonth },
    lifetime: { total: snap.callsAnswered.lifetime,  avgDuration: snap.avgCallDurationSeconds.lifetime },
  };
  res.json(map[range] || map.today);
});

// ── SOCKET.IO ─────────────────────────────────────────────────
io.on("connection", async (socket) => {
  console.log("Dashboard connected:", socket.id);
  const activeCalls = await getActiveCalls();
  socket.emit("live-update", {
    activeCalls,
    liveCount:  activeCalls.length,
    timestamp:  new Date().toISOString(),
  });
  socket.on("disconnect", () => console.log("Dashboard disconnected:", socket.id));
});

// ── STARTUP ───────────────────────────────────────────────────
(async () => {
  await redis.connect();
  console.log("Redis connected");
  if (WEBHOOK_SECRET) {
    console.log(`Webhook secured at: /vapi-webhook/${WEBHOOK_SECRET.slice(0, 4)}****`);
  } else {
    console.warn("WARN: VAPI_WEBHOOK_SECRET not set — webhook is unsecured.");
  }
  server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
})();
