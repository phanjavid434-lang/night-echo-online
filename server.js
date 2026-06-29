import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

const root = fileURLToPath(new URL(".", import.meta.url));
const port = Number(process.env.PORT || 8787);
const readyTimeoutMs = Number(process.env.READY_TIMEOUT_MS || 45000);
const rooms = new Map();

const mime = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".svg": "image/svg+xml"
};

function createLobby() {
  return { phase: "ready", ready: new Map(), votes: new Map(), waitingSince: new Map() };
}

function roomFor(name) {
  const key = (name || "echo").replace(/[^\w-]/g, "").slice(0, 32) || "echo";
  if (!rooms.has(key)) {
    const room = new Map();
    room.lobby = createLobby();
    rooms.set(key, room);
  }
  return [key, rooms.get(key)];
}

function ensureLobby(room) {
  if (!room.lobby) room.lobby = createLobby();
  return room.lobby;
}

function roomIds(room) {
  return [...room.keys()];
}

function roomHostId(room) {
  if (room.hostId && room.has(room.hostId)) return room.hostId;
  const next = [...room.values()].sort((a, b) => (a.joinedAt || 0) - (b.joinedAt || 0))[0];
  room.hostId = next ? next.id : null;
  return room.hostId;
}

function broadcast(room, payload, exceptId) {
  const message = JSON.stringify(payload);
  for (const [id, client] of room) {
    if (id === exceptId || client.ws.readyState !== client.ws.OPEN) continue;
    client.ws.send(message);
  }
}

function armReadyTimers(room, now = Date.now(), resetUnready = false) {
  const lobby = ensureLobby(room);
  if (lobby.phase !== "ready") return false;
  const ids = roomIds(room);
  const hasReady = ids.some((id) => lobby.ready.get(id));
  if (ids.length < 2 || !hasReady) {
    for (const id of ids) if (!lobby.ready.get(id)) lobby.waitingSince.set(id, now);
    return false;
  }
  for (const id of ids) {
    if (lobby.ready.get(id)) {
      lobby.waitingSince.delete(id);
    } else if (resetUnready || !lobby.waitingSince.has(id)) {
      lobby.waitingSince.set(id, now);
    }
  }
  return true;
}

function kickClient(room, target, reason = "kicked", by = null) {
  const kicked = room.get(target);
  if (!kicked || kicked.ws.readyState !== kicked.ws.OPEN) return false;
  kicked.ws.send(JSON.stringify({ type: "kicked", by, reason }));
  kicked.ws.close(4001, reason);
  return true;
}

function lobbyPayload(room) {
  const lobby = ensureLobby(room);
  const ids = roomIds(room);
  const now = Date.now();
  const hasReady = ids.some((id) => lobby.ready.get(id));
  return {
    type: "lobby",
    phase: lobby.phase,
    host: roomHostId(room),
    players: ids.map((id) => {
      const ready = !!lobby.ready.get(id);
      const waitLeft = lobby.phase === "ready" && ids.length >= 2 && hasReady && !ready
        ? Math.max(0, Math.ceil((readyTimeoutMs - (now - (lobby.waitingSince.get(id) || now))) / 1000))
        : null;
      return {
        id,
        ready,
        vote: lobby.votes.has(id) ? lobby.votes.get(id) : null,
        waitLeft
      };
    })
  };
}

function broadcastLobby(room) {
  broadcast(room, lobbyPayload(room));
}

function normalizeLevel(value) {
  const level = Math.round(Number(value));
  if (!Number.isFinite(level)) return null;
  return Math.max(1, Math.min(20, level));
}

function chooseVote(lobby, ids) {
  const counts = new Map();
  for (const id of ids) {
    const level = normalizeLevel(lobby.votes.get(id));
    if (!level) continue;
    counts.set(level, (counts.get(level) || 0) + 1);
  }
  let max = 0;
  for (const count of counts.values()) max = Math.max(max, count);
  const candidates = [...counts.entries()]
    .filter(([, count]) => count === max)
    .map(([level]) => level)
    .sort((a, b) => a - b);
  const level = candidates[Math.floor(Math.random() * candidates.length)] || 1;
  return {
    level,
    tied: candidates.length > 1,
    candidates,
    votes: Object.fromEntries([...counts.entries()].sort((a, b) => a[0] - b[0])),
    runSeed: randomUUID()
  };
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host}`);
    const requested = url.pathname === "/" ? "/index.html" : url.pathname;
    const safePath = normalize(decodeURIComponent(requested)).replace(/^(\.\.[/\\])+/, "");
    const filePath = join(root, safePath);
    if (!filePath.startsWith(root)) {
      res.writeHead(403);
      res.end("Forbidden");
      return;
    }
    const body = await readFile(filePath);
    res.writeHead(200, { "content-type": mime[extname(filePath)] || "application/octet-stream" });
    res.end(body);
  } catch {
    res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    res.end("Not found");
  }
});

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws, req) => {
  ws.isAlive = true;
  ws.on("pong", () => {
    ws.isAlive = true;
  });

  const url = new URL(req.url || "/ws", `http://${req.headers.host}`);
  const [roomName, room] = roomFor(url.searchParams.get("room"));
  const lobby = ensureLobby(room);
  const id = randomUUID();
  const client = { id, ws, state: null, joinedAt: Date.now() };
  room.set(id, client);
  if (!room.hostId || !room.has(room.hostId)) room.hostId = id;
  lobby.ready.set(id, false);
  lobby.waitingSince.set(id, Date.now());
  lobby.votes.delete(id);
  if (lobby.phase !== "vote") lobby.phase = "ready";
  armReadyTimers(room);

  ws.send(JSON.stringify({
    type: "welcome",
    id,
    room: roomName,
    lobby: lobbyPayload(room),
    players: [...room.values()]
      .filter((item) => item.id !== id && item.state)
      .map((item) => ({ id: item.id, state: item.state }))
  }));
  broadcast(room, { type: "join", id }, id);
  broadcastLobby(room);

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg.type === "state" && msg.state && typeof msg.state === "object") {
      client.state = { ...msg.state, t: Date.now() };
      broadcast(room, { type: "state", id, state: client.state }, id);
      return;
    }
    if (msg.type === "pulse" && msg.pulse && typeof msg.pulse === "object") {
      broadcast(room, { type: "pulse", id, pulse: msg.pulse }, id);
      return;
    }
    if (msg.type === "teamEvent" && msg.event && typeof msg.event === "object") {
      broadcast(room, { type: "teamEvent", id, event: { ...msg.event, t: Date.now() } }, id);
      return;
    }
    if (msg.type === "enemies" && msg.e && typeof msg.e === "object") {
      broadcast(room, { type: "enemies", id, e: msg.e, level: msg.level }, id);
      return;
    }
    if (msg.type === "kick") {
      const target = typeof msg.target === "string" ? msg.target : "";
      if (!target || target === id || id !== roomHostId(room)) return;
      kickClient(room, target, "kicked", id);
      return;
    }
    if (msg.type === "lobbyReady") {
      const lobby = ensureLobby(room);
      const now = Date.now();
      const hadReady = roomIds(room).some((playerId) => lobby.ready.get(playerId));
      const ready = !!msg.ready;
      lobby.ready.set(id, ready);
      if (ready) lobby.waitingSince.delete(id);
      else lobby.waitingSince.set(id, now);
      lobby.votes.delete(id);
      const ids = roomIds(room);
      const allReady = ids.length >= 2 && ids.every((playerId) => lobby.ready.get(playerId));
      if (allReady) {
        lobby.phase = "vote";
        lobby.votes.clear();
        lobby.waitingSince.clear();
      } else {
        lobby.phase = "ready";
        const hasReady = ids.some((playerId) => lobby.ready.get(playerId));
        armReadyTimers(room, now, !hadReady && hasReady);
      }
      broadcastLobby(room);
      return;
    }
    if (msg.type === "levelVote") {
      const level = normalizeLevel(msg.level);
      if (!level) return;
      const lobby = ensureLobby(room);
      lobby.phase = "vote";
      lobby.votes.set(id, level);
      broadcastLobby(room);

      const ids = roomIds(room);
      const allVoted = ids.length >= 2 && ids.every((playerId) => lobby.votes.has(playerId));
      if (allVoted) {
        const result = chooseVote(lobby, ids);
        lobby.phase = "playing";
        lobby.ready.clear();
        lobby.votes.clear();
        lobby.waitingSince.clear();
        for (const playerId of ids) lobby.ready.set(playerId, false);
        broadcast(room, { type: "levelChosen", ...result });
        broadcastLobby(room);
      }
    }
  });

  ws.on("close", () => {
    const lobby = ensureLobby(room);
    room.delete(id);
    if (room.hostId === id) room.hostId = null;
    lobby.ready.delete(id);
    lobby.waitingSince.delete(id);
    lobby.votes.delete(id);
    if (room.size === 0) {
      rooms.delete(roomName);
      return;
    }
    if (lobby.phase !== "playing") lobby.phase = "ready";
    broadcast(room, { type: "leave", id }, id);
    broadcastLobby(room);
  });
});

const readySweep = setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    const lobby = ensureLobby(room);
    if (lobby.phase !== "ready" || room.size < 2) continue;
    if (!armReadyTimers(room, now)) continue;
    for (const id of roomIds(room)) {
      if (lobby.ready.get(id)) continue;
      const since = lobby.waitingSince.get(id) || now;
      if (now - since >= readyTimeoutMs) kickClient(room, id, "readyTimeout");
    }
  }
}, 1000);

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 15000);

wss.on("close", () => {
  clearInterval(readySweep);
  clearInterval(heartbeat);
});

server.listen(port, () => {
  console.log(`Night Echo online MVP: http://localhost:${port}`);
});
