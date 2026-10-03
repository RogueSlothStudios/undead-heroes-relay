// Undead Heroes co-op relay. Phones/browsers can't host game servers, so the host player's game stays the
// authority and this server only forwards packets between the players in a room (5-letter code).
// Binary frames: [int32 LE peer id][payload]. Client->server: id = destination (1 = host, 0 = everyone else).
// Server->client: id = sender. Text frames are JSON control messages.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 5;
const MAX_ROOMS = 200;
const MAX_MSG = 256 * 1024;
const rooms = new Map(); // code -> { host, peers: Map<id, ws>, next }

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end(`Undead Heroes relay OK. rooms=${rooms.size}\n`);
});
const wss = new WebSocketServer({ server, maxPayload: MAX_MSG, perMessageDeflate: { threshold: 512 } });

function code() {
  const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  for (;;) {
    let c = '';
    for (let i = 0; i < 5; i++) c += A[Math.floor(Math.random() * A.length)];
    if (!rooms.has(c)) return c;
  }
}
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

wss.on('connection', (ws, req) => {
  const q = new URL(req.url, 'http://x').searchParams;
  const role = q.get('role');
  let room = null, id = 0;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  if (role === 'host') {
    if (rooms.size >= MAX_ROOMS) return ws.close(4003, 'server full');
    const want = String(q.get('want') || '').toUpperCase();
    const c = /^[A-Z0-9]{5}$/.test(want) && !rooms.has(want) ? want : code(); // fixed codes: automated tests
    room = { code: c, host: ws, peers: new Map(), next: 2 };
    rooms.set(c, room);
    id = 1;
    send(ws, { t: 'room', code: c, id: 1 });
  } else if (role === 'join') {
    room = rooms.get(String(q.get('code') || '').toUpperCase());
    if (!room) return ws.close(4004, 'room not found');
    if (room.peers.size >= MAX_PLAYERS + 1) return ws.close(4005, 'room full');
    id = room.next++;
    room.peers.set(id, ws);
    send(ws, { t: 'welcome', id, code: room.code });
    send(room.host, { t: 'peer', id });
  } else {
    return ws.close(4000, 'bad role');
  }
  ws.on('message', (data, isBinary) => {
    if (!isBinary || data.length < 4) return;
    const dest = data.readInt32LE(0);
    data.writeInt32LE(id, 0); // stamp the real sender
    if (id === 1) {
      if (dest > 1) { const p = room.peers.get(dest); if (p && p.readyState === 1) p.send(data); }
      else if (dest <= 0) for (const [pid, p] of room.peers) if (-dest !== pid && p.readyState === 1) p.send(data);
    } else if (dest === 1 || dest === 0) {
      if (room.host.readyState === 1) room.host.send(data);
    }
  });
  ws.on('close', () => {
    if (!room) return;
    if (id === 1) {
      rooms.delete(room.code);
      for (const p of room.peers.values()) p.close(4001, 'host left');
    } else {
      room.peers.delete(id);
      send(room.host, { t: 'gone', id });
    }
  });
});

// drop dead sockets (phones that lost signal) so hosts see the disconnect
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 15000);

server.listen(PORT, () => console.log(`relay listening on ${PORT}`));
