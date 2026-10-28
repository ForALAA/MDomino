const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const path = require('path');
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000
});
const PORT = process.env.PORT || 3000;
const rooms = new Map();
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size }));
function roomCode() { return crypto.randomBytes(3).toString('hex').toUpperCase(); }
function lobby(room) {
  return { type: 'lobby', roomCode: room.code, playerCount: room.playerCount, teamMode: room.teamMode,
    names: room.names.slice(), connected: room.players.filter(Boolean).length, started: room.started };
}
function broadcastLobby(room) { const packet = lobby(room); room.players.filter(Boolean).forEach(id => io.to(id).emit('packet', packet)); }
function fail(socket, event, error) { socket.emit(event, { ok: false, error }); }
io.on('connection', socket => {
  socket.on('create-room', data => {
    const count = Math.max(2, Math.min(4, Number(data?.playerCount) || 2));
    const teamMode = count === 4 && data?.teamMode === 'teams' ? 'teams' : 'solo';
    let code; do { code = roomCode(); } while (rooms.has(code));
    const names = Array.from({ length: count }, (_, i) => i === 0 ? String(data?.name || 'Player 1').slice(0, 18) : `Waiting for player ${i + 1}`);
    const room = { code, playerCount: count, teamMode, names, players: Array(count).fill(null), started: false, state: null, host: socket.id };
    room.players[0] = socket.id;
    room.state = { hands: [], placed: [], current: 0, names: names.slice(), playerCount: count, teamMode, round: 1,
      scores: Array(count).fill(0), roundWins: Array(count).fill(0), teams: teamMode === 'teams' ? [0,1,0,1] : Array.from({length: count}, (_, i) => i) };
    rooms.set(code, room); socket.data.roomCode = code; socket.data.playerIndex = 0;
    socket.emit('room-created', { ok: true, roomCode: code, state: room.state, lobby: lobby(room) });
  });
  socket.on('join-room', data => {
    const code = String(data?.roomCode || '').trim().toUpperCase(); const room = rooms.get(code);
    if (!room) return fail(socket, 'joined-room', 'Room not found. Check the room code.');
    if (room.started) return fail(socket, 'joined-room', 'This game has already started.');
    const index = room.players.findIndex((id, i) => i > 0 && !id);
    if (index < 0) return fail(socket, 'joined-room', 'Room is full.');
    room.players[index] = socket.id; room.names[index] = String(data?.name || `Player ${index + 1}`).slice(0, 18);
    socket.data.roomCode = code; socket.data.playerIndex = index;
    socket.emit('joined-room', { ok: true, roomCode: code, index, playerCount: room.playerCount, teamMode: room.teamMode, lobby: lobby(room) });
    broadcastLobby(room);
  });
  socket.on('packet', msg => {
    const code = socket.data.roomCode; const room = code && rooms.get(code); if (!room || !msg) return;
    const index = socket.data.playerIndex;
    if (msg.type === 'lobby') { broadcastLobby(room); return; }
    if (msg.type === 'state' && msg.state) {
      if (index === 0) {
        room.state = msg.state;
        if (msg.state.hands && msg.state.hands.length) room.started = true;
      } else {
        if (!room.started || !room.state || room.state.current !== index) return;
        room.state = msg.state;
      }
      room.names = (room.state.names || room.names).slice();
      room.players.filter(Boolean).forEach(id => io.to(id).emit('packet', { type: 'state', state: room.state }));
      broadcastLobby(room);
    }
  });
  socket.on('leave-room', () => leave(socket));
  socket.on('disconnect', () => leave(socket));
});
function leave(socket) {
  const code = socket.data.roomCode; const room = code && rooms.get(code); if (!room) return;
  const i = room.players.indexOf(socket.id); if (i >= 0) room.players[i] = null;
  if (i === 0) { room.players.filter(Boolean).forEach(id => io.to(id).emit('packet', { type: 'error', message: 'Host left the room.' })); rooms.delete(code); }
  else { room.names[i] = `Waiting for player ${i + 1}`; broadcastLobby(room); }
  delete socket.data.roomCode; delete socket.data.playerIndex;
}
server.listen(PORT, '0.0.0.0', () => console.log(`Dominoes server listening on ${PORT}`));
