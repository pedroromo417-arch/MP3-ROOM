// Servidor de salas para "Fiesta MP3".
// Sin dependencias: solo necesita Node 18 o más nuevo.
// Crea salas con código de 4 letras, hasta 4 jugadores por sala,
// y reenvía mensajes (chat, controles) entre los jugadores de la misma sala.

const http = require("http");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;
const MAX_ROOMS = 500;
const MAX_PAYLOAD = 4096;       // bytes por mensaje
const MAX_MSGS_PER_SEC = 150;   // por conexión
const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ"; // sin I ni O, igual que la interfaz
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

const rooms = new Map(); // code -> { code, slots: Array(4), host, started }
let nextId = 1;

// ---------- WebSocket mínimo ----------

function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) {
    head = Buffer.from([0x80 | op, len]);
  } else if (len < 65536) {
    head = Buffer.alloc(4);
    head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2);
  } else {
    head = Buffer.alloc(10);
    head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([head, payload]);
}

function send(c, obj) {
  if (c.closed) return;
  try { c.socket.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (_) {}
}

function closeConn(c, code) {
  if (c.closed) return;
  c.closed = true;
  try {
    const p = Buffer.alloc(2); p.writeUInt16BE(code || 1000, 0);
    c.socket.write(frame(8, p));
    c.socket.end();
  } catch (_) {}
  leaveRoom(c, true);
}

function onData(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = !!(b[0] & 0x80);
    const op = b[0] & 0x0f;
    const masked = !!(b[1] & 0x80);
    let len = b[1] & 0x7f;
    let off = 2;
    if (len === 126) {
      if (b.length < 4) return;
      len = b.readUInt16BE(2); off = 4;
    } else if (len === 127) {
      if (b.length < 10) return;
      const big = b.readBigUInt64BE(2);
      if (big > BigInt(MAX_PAYLOAD)) return closeConn(c, 1009);
      len = Number(big); off = 10;
    }
    if (len > MAX_PAYLOAD) return closeConn(c, 1009);
    if (!masked) return closeConn(c, 1002);
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4);
    const data = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
    c.buf = b.subarray(off + 4 + len);

    if (!fin || op === 0) return closeConn(c, 1003); // sin fragmentación
    if (op === 8) return closeConn(c, 1000);
    if (op === 9) { c.socket.write(frame(10, data)); continue; }
    if (op === 10) { c.alive = true; continue; }
    if (op !== 1) return closeConn(c, 1003);       // solo texto

    c.count++;
    if (c.count > MAX_MSGS_PER_SEC) return closeConn(c, 1008);
    let msg;
    try { msg = JSON.parse(data.toString("utf8")); } catch (_) { send(c, { t: "error", msg: "Mensaje inválido." }); continue; }
    if (!msg || typeof msg !== "object") continue;
    handle(c, msg);
  }
}

// ---------- Salas ----------

function newCode() {
  for (let n = 0; n < 50; n++) {
    let s = "";
    for (let i = 0; i < 4; i++) s += ALPHABET[crypto.randomInt(ALPHABET.length)];
    if (!rooms.has(s)) return s;
  }
  return null;
}

function cleanName(v, slot) {
  const s = typeof v === "string" ? v.replace(/[\u0000-\u001f<>]/g, "").trim().slice(0, 16) : "";
  return s || "Jugador " + slot;
}

function playersOf(room) {
  const out = [];
  room.slots.forEach((c, i) => {
    if (c) out.push({ slot: i + 1, name: c.name, ready: c.ready, host: room.host === c });
  });
  return out;
}

function broadcast(room, obj, except) {
  room.slots.forEach((c) => { if (c && c !== except) send(c, obj); });
}

function joinRoom(c, room, name) {
  const idx = room.slots.findIndex((s) => !s);
  if (idx < 0) return send(c, { t: "error", msg: "La sala está llena (máximo 4)." });
  c.room = room; c.slot = idx + 1; c.ready = false;
  c.name = cleanName(name, c.slot);
  room.slots[idx] = c;
  if (!room.host) room.host = c;
  send(c, { t: "joined", code: room.code, slot: c.slot, host: room.host === c, players: playersOf(room) });
  broadcast(room, { t: "players", players: playersOf(room) }, c);
}

function leaveRoom(c, silent) {
  const room = c.room;
  if (!room) return;
  room.slots[c.slot - 1] = null;
  c.room = null;
  const left = room.slots.filter(Boolean);
  if (left.length === 0) { rooms.delete(room.code); return; }
  if (room.host === c) room.host = left[0];
  if (room.started && left.length < 2) room.started = false;
  broadcast(room, { t: "players", players: playersOf(room), left: c.slot, host: room.slots.indexOf(room.host) + 1 });
  if (!silent) send(c, { t: "left" });
}

function handle(c, m) {
  switch (m.t) {
    case "create": {
      if (c.room) return send(c, { t: "error", msg: "Ya estás en una sala." });
      if (rooms.size >= MAX_ROOMS) return send(c, { t: "error", msg: "Servidor lleno. Intenta más tarde." });
      const code = newCode();
      if (!code) return send(c, { t: "error", msg: "No se pudo crear la sala." });
      const room = { code, slots: Array(MAX_PLAYERS).fill(null), host: null, started: false };
      rooms.set(code, room);
      joinRoom(c, room, m.name);
      break;
    }
    case "join": {
      if (c.room) return send(c, { t: "error", msg: "Ya estás en una sala." });
      const code = typeof m.code === "string" ? m.code.trim().toUpperCase() : "";
      const room = rooms.get(code);
      if (!room) return send(c, { t: "error", msg: "No existe la sala " + (code || "----") + "." });
      if (room.started) return send(c, { t: "error", msg: "Esa partida ya empezó." });
      joinRoom(c, room, m.name);
      break;
    }
    case "leave":
      leaveRoom(c, false);
      break;
    case "ready":
      if (!c.room || c.room.started) return;
      c.ready = !!m.value;
      broadcast(c.room, { t: "players", players: playersOf(c.room) });
      break;
    case "chat": {
      if (!c.room) return;
      const text = typeof m.text === "string" ? m.text.replace(/[\u0000-\u001f]/g, " ").trim().slice(0, 200) : "";
      if (text) broadcast(c.room, { t: "chat", slot: c.slot, name: c.name, text });
      break;
    }
    case "start": {
      const room = c.room;
      if (!room) return;
      if (room.host !== c) return send(c, { t: "error", msg: "Solo el anfitrión puede empezar." });
      if (room.started) return;
      const n = room.slots.filter(Boolean).length;
      if (n < 2) return send(c, { t: "error", msg: "Se necesitan al menos 2 jugadores." });
      room.started = true;
      // Semilla común: todos los dispositivos pueden arrancar con el mismo valor.
      const seed = crypto.randomInt(0, 2 ** 31);
      broadcast(room, { t: "start", seed, players: playersOf(room) });
      break;
    }
    case "input": {
      // Reenvía los botones de este jugador a los demás: { t:"input", frame, data }
      const room = c.room;
      if (!room || !room.started) return;
      broadcast(room, { t: "input", slot: c.slot, frame: m.frame, data: m.data }, c);
      break;
    }
    case "ping":
      send(c, { t: "pong", ts: m.ts });
      break;
    default:
      send(c, { t: "error", msg: "Mensaje desconocido." });
  }
}

// ---------- HTTP + upgrade ----------

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("Fiesta MP3: servidor de salas activo. Salas: " + rooms.size + "\n");
});

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key || (req.headers.upgrade || "").toLowerCase() !== "websocket") {
    socket.end("HTTP/1.1 400 Bad Request\r\n\r\n");
    return;
  }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\n" +
    "Upgrade: websocket\r\nConnection: Upgrade\r\n" +
    "Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
  );
  socket.setNoDelay(true);
  const c = { id: nextId++, socket, buf: Buffer.alloc(0), room: null, slot: 0, name: "", ready: false, alive: true, closed: false, count: 0 };
  socket.on("data", (d) => onData(c, d));
  socket.on("close", () => { c.closed = true; leaveRoom(c, true); });
  socket.on("error", () => { c.closed = true; leaveRoom(c, true); socket.destroy(); });
  send(c, { t: "hello", maxPlayers: MAX_PLAYERS });
  all.add(c);
  socket.on("close", () => all.delete(c));
});

const all = new Set();
setInterval(() => { all.forEach((c) => { c.count = 0; }); }, 1000);
setInterval(() => {
  all.forEach((c) => {
    if (!c.alive) { c.closed = true; leaveRoom(c, true); c.socket.destroy(); all.delete(c); return; }
    c.alive = false;
    try { c.socket.write(frame(9, Buffer.alloc(0))); } catch (_) {}
  });
}, 30000);

server.listen(PORT, () => console.log("Servidor de salas en el puerto " + PORT));
