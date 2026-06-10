/**
 * Sarah Live Dashboard — server.js
 * Railway + Redis + Socket.IO
 * Tracks Vapi calls only. No Make.com dependency.
 */
const express    = require("express");
const http       = require("http");
const { Server } = require("socket.io");
const cors       = require("cors");
const { createClient } = require("redis");

const PORT           = process.env.PORT || 3000;
const REDIS_URL      = process.env.REDIS_URL;
const WEBHOOK_SECRET = process.env.VAPI_WEBHOOK_SECRET;

const app    = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

app.use(cors());
app.use(express.json({ limit: "2mb" }));

if (!REDIS_URL) console.warn("WARN: REDIS_URL is not set.");
const redis = createClient({ url: REDIS_URL });
redis.on("error", (err) => console.error("Redis error:", err));

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
function calcDurationSeconds(call) {
  const dur =
    Number(call?.duration ?? 0) ||
    Number(call?.durationSeconds ?? 0) ||
    (call?.durationMs ? Number(call.durationMs) / 1000 : 0) ||
    (call?.startedAt && call?.endedAt
      ? (new Date(call.endedAt) - new Date(call.startedAt)) / 1000
      : 0);
  return Number.isFinite(dur) ? Math.max(0, dur) : 0;
}
function isAnsweredCall(call) {
  const dur = calcDurationSeconds(call);
  return dur >= 0;
}

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

async function recordAnsweredCall(call) {
  if (!isAnsweredCall(call)) return;
  const duration = calcDurationSeconds(call);
  const endedAt  = call?.endedAt ? new Date(call.endedAt) : new Date();
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
  const rows  = await multi.exec();
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

async function broadcastLiveUpdate() {
  const activeCalls = await getActiveCalls();
  io.emit("live-update", {
    activeCalls,
    liveCount:  activeCalls.length,
    timestamp:  new Date().toISOString(),
  });
}

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

async function handleVapiWebhook(body) {
  const msg = body?.message;
  if (!msg) return;
  if (msg.type !== "status-update" && msg.type !== "end-of-call-report") return;
  const raw    = msg.call || {};
  const callId = raw.id;
  if (!callId) return;
  const call = {
    id:           callId,
    assistantId:  raw.assistantId  || null,
    callerNumber: raw?.customer?.number || "Unknown",
    startedAt:    raw.startedAt    || null,
    endedAt:      raw.endedAt      || null,
    endedReason:  raw.endedReason  || null,
    duration:     calcDurationSeconds(raw),
  };
  console.log(`VAPI [${msg.type}] callId=${callId} status=${msg.status || "n/a"} dur=${call.duration}s`);
  if (msg.type === "status-update") {
    const status = msg.status;
    if (status === "ringing" || status === "in-progress") {
      await setActiveCall(callId, { ...call, status, startedAt: call.startedAt || new Date().toISOString() });
      await broadcastLiveUpdate();
    }
    if (status === "ended") {
      await removeActiveCall(callId);
      await broadcastLiveUpdate();
    }
  }
  if (msg.type === "end-of-call-report") {
    await removeActiveCall(callId);
    await recordAnsweredCall(call);
    await broadcastLiveUpdate();
  }
}

app.post("/vapi-webhook", (req, res) => {
  if (WEBHOOK_SECRET) return res.sendStatus(403);
  res.sendStatus(200);
  handleVapiWebhook(req.body).catch((e) => console.error("Webhook error:", e));
});

app.post("/vapi-webhook/:secret", (req, res) => {
  if (WEBHOOK_SECRET && req.params.secret !== WEBHOOK_SECRET) {
    return res.sendStatus(401);
  }
  res.sendStatus(200);
  handleVapiWebhook(req.body).catch((e) => console.error("Webhook error:", e));
});

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

app.get("/debug/metrics-keys", async (req, res) => {
  const keys        = await redis.keys("metrics:*");
  const activeCalls = await redis.hGetAll("activeCalls");
  res.json({ keys, activeCallCount: Object.keys(activeCalls).length });
});

// ── GOOGLE REVIEWS ────────────────────────────────────────────
app.get("/google", async (req, res) => {
  const PLACES_KEY  = process.env.GOOGLE_PLACES_KEY;
  const PLACE_ID    = process.env.GOOGLE_PLACE_ID;
  const GHL_TOKEN   = process.env.GHL_TOKEN;
  const GHL_LOC     = process.env.GHL_LOCATION;
  const WORKFLOW_ID = process.env.GHL_REVIEW_WORKFLOW;

  try {
    // Fetch Google Places rating + review count (New Places API)
    const placesUrl = `https://places.googleapis.com/v1/places/ChIJCTEpLmsNI9STEBM`;
    console.log("Places URL:", placesUrl);
    const placesRes = await fetch(placesUrl, {
      headers: {
        "X-Goog-Api-Key": PLACES_KEY,
        "X-Goog-FieldMask": "rating,userRatingCount",
        "Content-Type": "application/json"
      }
    });
    const placesData = await placesRes.json();
    console.log("Places response:", JSON.stringify(placesData));
    const rating       = placesData.rating ?? null;
    const totalReviews = placesData.userRatingCount ?? null;

    // Count contacts enrolled in review workflow via contacts search
    const contactsUrl = `https://services.leadconnectorhq.com/contacts/?locationId=${GHL_LOC}&query=&workflowId=${WORKFLOW_ID}&limit=1`;
    console.log("Contacts URL:", contactsUrl);
    const contactsRes = await fetch(contactsUrl, {
      headers: { Authorization: `Bearer ${GHL_TOKEN}`, Version: "2021-07-28" }
    });
    const contactsData = await contactsRes.json();
    console.log("Contacts response:", JSON.stringify(contactsData).slice(0, 300));
    const requestsSent = contactsData?.meta?.total ?? null;

    // Calculate conversion rate
    const conversion = requestsSent && totalReviews
      ? Math.min(100, Math.round((totalReviews / requestsSent) * 100))
      : null;

    res.json({ rating, totalReviews, requestsSent, conversion });
  } catch(e) {
    console.error("Google endpoint error:", e);
    res.status(500).json({ error: "Failed to fetch review data" });
  }
});

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

(async () => {
  await redis.connect();
  console.log("Redis connected");
  if (WEBHOOK_SECRET) {
    console.log(`Secured webhook: /vapi-webhook/${WEBHOOK_SECRET.slice(0, 4)}****`);
  } else {
    console.warn("WARN: VAPI_WEBHOOK_SECRET not set — webhook is unsecured.");
  }
  server.listen(PORT, () => {
    console.log(`Sarah Live Server running on port ${PORT}`);
  });
})();
