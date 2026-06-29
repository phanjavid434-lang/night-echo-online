import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

const root = fileURLToPath(new URL(".", import.meta.url));
const port = Number(process.env.PORT || 8787);
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
  return { phase: "ready", ready: new Map(), votes: new Map() };
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

function broadcast(room, payload, exceptId) {
  const message = JSON.stringify(payload);
  for (const [id, client] of room) {
    if (id === exceptId || client.ws.readyState !== client.ws.OPEN) continue;
    client.ws.send(message);
  }
}

function lobbyPayload(room) {
  const lobby = ensureLobby(room);
  return {
    type: "lobby",
    phase: lobby.phase,
    players: roomIds(room).map((id) => ({
      id,
      ready: !!lobby.ready.get(id),
      vote: lobby.votes.has(id) ? lobby.votes.get(id) : null
    }))
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
  lobby.ready.set(id, false);
  lobby.votes.delete(id);
  if (lobby.phase !== "vote") lobby.phase = "ready";

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
    if (msg.type === "lobbyReady") {
      const lobby = ensureLobby(room);
      lobby.ready.set(id, !!msg.ready);
      lobby.votes.delete(id);
      const ids = roomIds(room);
      const allReady = ids.length >= 2 && ids.every((playerId) => lobby.ready.get(playerId));
      if (allReady) {
        lobby.phase = "vote";
        lobby.votes.clear();
      } else {
        lobby.phase = "ready";
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
        for (const playerId of ids) lobby.ready.set(playerId, false);
        broadcast(room, { type: "levelChosen", ...result });
        broadcastLobby(room);
      }
    }
  });

  ws.on("close", () => {
    const lobby = ensureLobby(room);
    room.delete(id);
    lobby.ready.delete(id);
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

wss.on("close", () => clearInterval(heartbeat));

server.listen(port, () => {
  console.log(`Night Echo online MVP: http://localhost:${port}`);
});
