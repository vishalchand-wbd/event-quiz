'use strict';

/**
 * Real-time quiz server.
 *
 * One Node process, in-memory state, JSONL persistence for crash recovery.
 *
 * State machine:
 *   lobby --advance--> active --(timer expires)--> closed --reveal--> revealed
 *                                  \                                       |
 *                                   \--(early reveal)-----------------/    |
 *                                                                          |
 *   revealed --advance--> active(next)  OR  finished
 *
 *   ANY state --host:reset--> lobby (clears all players, scores, timers,
 *                                    and rotates the room code)
 *
 * Thirteen Socket.IO events:
 *   Client → Server: player:join, player:answer, host:advance, host:reveal, host:reset
 *   Server → Client: state:lobby, state:question, state:answerCount,
 *                    state:reveal, state:finished, state:reset, state:meta, error
 *
 * Room code:
 *   - 4-digit numeric code generated once at boot (or restored from JSONL replay)
 *   - Required on player:join; mismatch → INVALID_CODE
 *   - Rotated on host:reset so leaked codes don't carry over to next game
 *   - Broadcast via state:meta on connect and after reset
 *
 * Late-join rules:
 *   - NEW players joining when state != LOBBY → rejected with GAME_STARTED
 *   - EXISTING players (already in game.players from lobby) → allowed to rejoin
 *     from any state. They get snapped to the current state.
 *
 * Player identity:
 *   - Email is the canonical identity (lookups, idempotency, late-join check).
 *   - firstName (required) and lastName (optional) are stored alongside but
 *     are display-only — the UI shows names instead of email everywhere.
 *   - Validation accepts Unicode letters + diacritics + standard name
 *     punctuation; max 40 chars per field after trimming.
 *   - player:join error codes (client maps these to user-facing copy):
 *       INVALID_CODE, GAME_STARTED, EMAIL_DOMAIN,
 *       FIRST_NAME_REQUIRED, INVALID_FIRST_NAME, INVALID_LAST_NAME
 */

const http = require('http');
const path = require('path');
const fs = require('fs');
const express = require('express');
const { Server } = require('socket.io');

const persistence = require('./persistence');

// =====================================================================
// CONFIGURATION (override via environment variables in the systemd unit)
// =====================================================================

const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST_TOKEN = process.env.HOST_TOKEN || '';
const ALLOWED_EMAIL_DOMAIN = process.env.ALLOWED_EMAIL_DOMAIN || '';
const STATE_FILE = process.env.STATE_FILE || '/var/lib/quiz/state.jsonl';
const QUIZ_FILE = process.env.QUIZ_FILE || path.join(__dirname, 'quiz.json');
const PUBLIC_DIR = path.join(__dirname, 'public');

// Throttle for state:answerCount broadcasts. 250ms = max 4 emits/sec, per spec.
const ANSWER_COUNT_THROTTLE_MS = 250;

// Fail fast if required secrets are missing — better than booting in a half-broken state.
if (!HOST_TOKEN) {
  console.error('FATAL: HOST_TOKEN environment variable not set.');
  process.exit(1);
}
if (!ALLOWED_EMAIL_DOMAIN) {
  console.error('FATAL: ALLOWED_EMAIL_DOMAIN environment variable not set.');
  process.exit(1);
}

// =====================================================================
// QUIZ DATA (loaded once at boot from quiz.json)
// =====================================================================

const quiz = JSON.parse(fs.readFileSync(QUIZ_FILE, 'utf8'));
validateQuiz(quiz);

function validateQuiz(q) {
  if (!Array.isArray(q.questions) || q.questions.length === 0) {
    throw new Error('quiz.json must have a non-empty "questions" array');
  }
  q.questions.forEach((qst, i) => {
    if (typeof qst.text !== 'string') {
      throw new Error(`Question ${i}: "text" must be a string`);
    }
    if (!Array.isArray(qst.options) || qst.options.length !== 4) {
      throw new Error(`Question ${i}: "options" must be an array of length 4`);
    }
    if (typeof qst.correctOption !== 'number' || qst.correctOption < 0 || qst.correctOption > 3) {
      throw new Error(`Question ${i}: "correctOption" must be an integer 0-3`);
    }
    if (typeof qst.durationMs !== 'number' || qst.durationMs < 5000 || qst.durationMs > 60000) {
      throw new Error(`Question ${i}: "durationMs" must be 5000-60000`);
    }
  });
}

// =====================================================================
// STATE MACHINE
// =====================================================================

const STATE = Object.freeze({
  LOBBY: 'lobby',
  ACTIVE: 'active',         // question shown, timer running, accepting answers
  CLOSED: 'closed',         // timer expired (or early-revealed); no more answers
  REVEALED: 'revealed',     // results shown; waiting for host to advance
  FINISHED: 'finished',
});

// All mutable game state lives in this single object — easy to reason about.
const game = {
  state: STATE.LOBBY,
  roomCode: null,             // 4-digit string. null until ensureRoomCode() runs (boot).
  currentIndex: -1,           // index of the current question (-1 = no question yet)
  currentStartedAt: null,     // server timestamp (ms) when current question started
  currentEndsAt: null,        // server timestamp (ms) when current question's timer expires
  players: new Map(),         // email -> { email, firstName, lastName, joinedAt, score, answers: Map }
  currentAnswers: new Map(),  // email -> answer record, JUST for the current question
  closeTimer: null,           // setTimeout handle for auto-close on timer expiry
};

// =====================================================================
// ROOM CODE
// =====================================================================

function generateRoomCode() {
  // Range 1000-9999 — avoids leading-zero ambiguity when displayed.
  return String(1000 + Math.floor(Math.random() * 9000));
}

// =====================================================================
// SCORING (deterministic — same inputs always yield same output)
// =====================================================================

function calculateScore(correct, elapsedMs, durationMs) {
  if (!correct) return 0;
  // Linear decay from 1000 (instant) down to 500 (at full duration).
  // Spec: max(500, 1000 - floor(elapsedMs / durationMs * 500))
  const raw = 1000 - Math.floor((elapsedMs / durationMs) * 500);
  return Math.max(500, raw);
}

// =====================================================================
// LEADERBOARD
//   Sort by score DESC, then by email ASC for deterministic tie-breaking.
// =====================================================================

function getLeaderboard(topN) {
  const sorted = Array.from(game.players.values())
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return a.email.localeCompare(b.email);
    });
  return topN == null ? sorted : sorted.slice(0, topN);
}

function getRankFor(email) {
  const ranked = getLeaderboard();
  for (let i = 0; i < ranked.length; i++) {
    if (ranked[i].email === email) return i + 1;
  }
  return null;
}

// =====================================================================
// EVENT APPLICATION
// =====================================================================

function applyEvent(event) {
  switch (event.type) {
    case 'join': {
      // Players are keyed by email. Re-joining = no-op for the player record
      // overall, but we backfill names if the existing record was created
      // under an older schema (pre-A1) and they're rejoining with names.
      const existing = game.players.get(event.email);
      if (!existing) {
        game.players.set(event.email, {
          email: event.email,
          firstName: event.firstName || '',
          lastName: event.lastName || '',
          joinedAt: event.ts,
          score: 0,
          answers: new Map(),
        });
      } else {
        if (!existing.firstName && event.firstName) existing.firstName = event.firstName;
        if (!existing.lastName && event.lastName) existing.lastName = event.lastName;
      }
      break;
    }

    case 'question_started': {
      game.state = STATE.ACTIVE;
      game.currentIndex = event.index;
      game.currentStartedAt = event.ts;
      game.currentEndsAt = event.ts + quiz.questions[event.index].durationMs;
      game.currentAnswers = new Map();
      break;
    }

    case 'answer': {
      const player = game.players.get(event.email);
      if (!player) break;                            // unknown player — drop
      if (player.answers.has(event.qIndex)) break;   // already answered — idempotent

      const q = quiz.questions[event.qIndex];
      const elapsedMs = event.ts - game.currentStartedAt;
      const correct = event.option === q.correctOption;
      const scoreAwarded = calculateScore(correct, elapsedMs, q.durationMs);

      const record = {
        option: event.option,
        serverTs: event.ts,
        scoreAwarded,
        correct,
      };
      player.answers.set(event.qIndex, record);
      player.score += scoreAwarded;

      if (event.qIndex === game.currentIndex) {
        game.currentAnswers.set(event.email, record);
      }
      break;
    }

    case 'question_closed': {
      game.state = STATE.CLOSED;
      break;
    }

    case 'question_revealed': {
      game.state = STATE.REVEALED;
      break;
    }

    case 'finished': {
      game.state = STATE.FINISHED;
      break;
    }

    case 'reset': {
      game.state = STATE.LOBBY;
      game.currentIndex = -1;
      game.currentStartedAt = null;
      game.currentEndsAt = null;
      game.players = new Map();
      game.currentAnswers = new Map();
      break;
    }

    case 'room_code_set': {
      game.roomCode = event.code;
      break;
    }

    default:
      console.warn('applyEvent: unknown event type:', event.type);
  }
}

// =====================================================================
// RECORD-AND-PERSIST helper
// =====================================================================

function recordAndPersist(event) {
  applyEvent(event);
  persistence.append(event);
}

// =====================================================================
// PAYLOAD BUILDERS (what we send to clients)
// =====================================================================

function buildQuestionPayload() {
  const q = quiz.questions[game.currentIndex];
  return {
    index: game.currentIndex,
    total: quiz.questions.length,
    text: q.text,
    options: q.options,
    startsAt: game.currentStartedAt,
    endsAt: game.currentEndsAt,
    serverNow: Date.now(),
  };
}

function buildRevealPayload() {
  const q = quiz.questions[game.currentIndex];
  const distribution = [0, 0, 0, 0];
  let totalAnswered = 0;
  let correctAnswered = 0;

  for (const ans of game.currentAnswers.values()) {
    distribution[ans.option]++;
    totalAnswered++;
    if (ans.correct) correctAnswered++;
  }

  const pctCorrect = totalAnswered > 0
    ? Math.round((correctAnswered / totalAnswered) * 100)
    : 0;

  return {
    index: game.currentIndex,
    questionText: q.text,
    options: q.options,
    correctOption: q.correctOption,
    distribution,
    totalAnswered,
    pctCorrect,
    leaderboard: getLeaderboard(5),
  };
}

function buildPlayerRevealPayload(email, publicCache, rankByEmail) {
  const base = publicCache || buildRevealPayload();
  const rank = rankByEmail ? (rankByEmail.get(email) || null) : getRankFor(email);

  const playerAnswer = game.currentAnswers.get(email);
  const player = game.players.get(email);

  const yourResult = playerAnswer
    ? {
        correct: playerAnswer.correct,
        pointsAwarded: playerAnswer.scoreAwarded,
        yourOption: playerAnswer.option,
        totalScore: player ? player.score : 0,
        rank,
      }
    : {
        correct: false,
        pointsAwarded: 0,
        yourOption: null,
        totalScore: player ? player.score : 0,
        rank,
      };

  return Object.assign({}, base, { yourResult });
}

// =====================================================================
// BROADCASTS
// =====================================================================

function broadcastState() {
  switch (game.state) {
    case STATE.LOBBY:
      io.emit('state:lobby', { playerCount: game.players.size });
      break;

    case STATE.ACTIVE:
    case STATE.CLOSED:
      io.emit('state:question', buildQuestionPayload());
      break;

    case STATE.REVEALED: {
      const publicReveal = buildRevealPayload();
      // Precompute the rank lookup once (the O(N²) fix from the load test).
      const sortedAll = getLeaderboard();
      const rankByEmail = new Map();
      for (let i = 0; i < sortedAll.length; i++) {
        rankByEmail.set(sortedAll[i].email, i + 1);
      }
      io.to('board').emit('state:reveal', publicReveal);
      io.to('host').emit('state:reveal', publicReveal);
      for (const [, sock] of io.of('/').sockets) {
        if (sock.data.role === 'player' && sock.data.email) {
          sock.emit('state:reveal', buildPlayerRevealPayload(sock.data.email, publicReveal, rankByEmail));
        }
      }
      break;
    }

    case STATE.FINISHED:
      io.emit('state:finished', { podium: getLeaderboard(5) });
      break;
  }
}

function sendCurrentStateTo(socket) {
  // Always send the room code first, before any state event. Clients (esp.
  // /board) need it to render the lobby header regardless of game phase.
  socket.emit('state:meta', { code: game.roomCode });

  switch (game.state) {
    case STATE.LOBBY:
      socket.emit('state:lobby', { playerCount: game.players.size });
      break;
    case STATE.ACTIVE:
    case STATE.CLOSED:
      socket.emit('state:question', buildQuestionPayload());
      socket.emit('state:answerCount', {
        count: game.currentAnswers.size,
        total: game.players.size,
        index: game.currentIndex,
      });
      break;
    case STATE.REVEALED:
      if (socket.data.role === 'player' && socket.data.email) {
        socket.emit('state:reveal', buildPlayerRevealPayload(socket.data.email));
      } else {
        socket.emit('state:reveal', buildRevealPayload());
      }
      break;
    case STATE.FINISHED:
      socket.emit('state:finished', { podium: getLeaderboard(5) });
      break;
  }
}

// =====================================================================
// THROTTLED ANSWER COUNT BROADCASTS (max 4/sec)
// =====================================================================

let answerCountTimer = null;

function scheduleAnswerCountBroadcast() {
  if (answerCountTimer) return;
  answerCountTimer = setTimeout(() => {
    answerCountTimer = null;
    if (game.state === STATE.ACTIVE || game.state === STATE.CLOSED) {
      io.emit('state:answerCount', {
        count: game.currentAnswers.size,
        total: game.players.size,
        index: game.currentIndex,
      });
    }
  }, ANSWER_COUNT_THROTTLE_MS);
}

// =====================================================================
// THROTTLED LOBBY COUNT BROADCASTS
// =====================================================================

let lobbyBroadcastTimer = null;

function scheduleLobbyBroadcast() {
  if (lobbyBroadcastTimer) return;
  lobbyBroadcastTimer = setTimeout(() => {
    lobbyBroadcastTimer = null;
    if (game.state === STATE.LOBBY) {
      io.emit('state:lobby', { playerCount: game.players.size });
    }
  }, ANSWER_COUNT_THROTTLE_MS);
}

// =====================================================================
// TIMER MANAGEMENT
// =====================================================================

function scheduleAutoClose() {
  if (game.closeTimer) {
    clearTimeout(game.closeTimer);
    game.closeTimer = null;
  }
  const remainingMs = game.currentEndsAt - Date.now();
  if (remainingMs <= 0) {
    closeCurrentQuestion();
  } else {
    game.closeTimer = setTimeout(closeCurrentQuestion, remainingMs);
  }
}

function closeCurrentQuestion() {
  if (game.state !== STATE.ACTIVE) return;
  recordAndPersist({
    type: 'question_closed',
    ts: Date.now(),
    index: game.currentIndex,
  });
  io.emit('state:answerCount', {
    count: game.currentAnswers.size,
    total: game.players.size,
    index: game.currentIndex,
    final: true,
  });
}

// =====================================================================
// HOST ACTIONS
// =====================================================================

function hostAdvance() {
  if (game.state !== STATE.LOBBY && game.state !== STATE.REVEALED) {
    throw new Error(`Cannot advance from state "${game.state}"`);
  }
  const nextIndex = game.currentIndex + 1;
  if (nextIndex >= quiz.questions.length) {
    recordAndPersist({ type: 'finished', ts: Date.now() });
    broadcastState();
    return;
  }
  recordAndPersist({ type: 'question_started', ts: Date.now(), index: nextIndex });
  scheduleAutoClose();
  broadcastState();
}

function hostReveal() {
  if (game.state === STATE.ACTIVE) {
    if (game.closeTimer) {
      clearTimeout(game.closeTimer);
      game.closeTimer = null;
    }
    recordAndPersist({
      type: 'question_closed',
      ts: Date.now(),
      index: game.currentIndex,
    });
  }
  if (game.state !== STATE.CLOSED) {
    throw new Error(`Cannot reveal from state "${game.state}"`);
  }
  recordAndPersist({
    type: 'question_revealed',
    ts: Date.now(),
    index: game.currentIndex,
  });
  broadcastState();
}

function hostReset() {
  if (game.closeTimer) {
    clearTimeout(game.closeTimer);
    game.closeTimer = null;
  }
  if (answerCountTimer) {
    clearTimeout(answerCountTimer);
    answerCountTimer = null;
  }
  if (lobbyBroadcastTimer) {
    clearTimeout(lobbyBroadcastTimer);
    lobbyBroadcastTimer = null;
  }

  recordAndPersist({ type: 'reset', ts: Date.now() });

  // Rotate the room code so leaked codes don't carry over.
  const newCode = generateRoomCode();
  recordAndPersist({ type: 'room_code_set', ts: Date.now(), code: newCode });
  console.log('host:reset: new room code is', newCode);

  // Clear cached identity from every connected player socket.
  for (const [, sock] of io.of('/').sockets) {
    if (sock.data.role === 'player') {
      sock.data.email = null;
    }
  }

  io.emit('state:reset', {});
  io.emit('state:meta', { code: game.roomCode });
  broadcastState();
}

// =====================================================================
// HTTP / SOCKET.IO SETUP
// =====================================================================

const app = express();
app.use(express.static(PUBLIC_DIR));
app.get('/', (_req, res) => res.redirect('/play'));
app.get('/play', (_req, res) => res.sendFile(path.join(PUBLIC_DIR, 'play.html')));
app.get('/host', (_req, res) => res.sendFile(path.join(PUBLIC_DIR, 'host.html')));
app.get('/board', (_req, res) => res.sendFile(path.join(PUBLIC_DIR, 'board.html')));
app.get('/health', (_req, res) => res.json({
  status: 'ok',
  state: game.state,
  roomCode: game.roomCode,
  currentIndex: game.currentIndex,
  playerCount: game.players.size,
  uptimeSec: Math.round(process.uptime()),
}));

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  transports: ['websocket'],
  pingInterval: 25000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e6,
});

// =====================================================================
// EMAIL & NAME VALIDATION
// =====================================================================

const escapedDomain = ALLOWED_EMAIL_DOMAIN.replace(/\./g, '\\.');
const EMAIL_REGEX = new RegExp(`^[A-Za-z0-9._%+-]+@${escapedDomain}$`, 'i');

// Names: allow letters from any script (\p{L}), combining marks for diacritics
// (\p{M}), space, hyphen, ASCII apostrophe, curly apostrophe (U+2019), period.
// This lets through O'Brien, García, Müller, D'Souza, St. John, Anne-Marie,
// देवी, محمد, 王明 etc., while rejecting digits, punctuation, emoji, etc.
// Length 1-40 chars after trimming.
const NAME_REGEX = /^[\p{L}\p{M}\s\-'\u2019.]+$/u;

function isValidName(s) {
  if (typeof s !== 'string') return false;
  return s.length >= 1 && s.length <= 40 && NAME_REGEX.test(s);
}

// =====================================================================
// SOCKET HANDLERS
// =====================================================================

io.on('connection', (socket) => {
  const auth = socket.handshake.auth || {};

  if (auth.role === 'host') {
    if (auth.token !== HOST_TOKEN) {
      socket.emit('error', { message: 'Invalid host token' });
      socket.disconnect(true);
      return;
    }
    socket.data.role = 'host';
    socket.join('host');
    sendCurrentStateTo(socket);
  } else if (auth.role === 'board') {
    socket.data.role = 'board';
    socket.join('board');
    sendCurrentStateTo(socket);
  } else {
    socket.data.role = 'player';
    // Send the room code immediately so the player UI can render header info
    // before they bother filling out the join form.
    socket.emit('state:meta', { code: game.roomCode });
  }

  // ----- Player events -----

  socket.on('player:join', (payload, ack) => {
    if (socket.data.role !== 'player') {
      return safeAck(ack, { ok: false, error: 'Not a player socket' });
    }

    // 1. Code must match the active room code.
    const submittedCode = String((payload && payload.code) || '').trim();
    if (!submittedCode || submittedCode !== game.roomCode) {
      return safeAck(ack, { ok: false, error: 'INVALID_CODE' });
    }

    // 2. Email must match the allowed domain. Client renders the user-facing
    //    copy (so corp wording can change without a server push).
    const email = String((payload && payload.email) || '').trim().toLowerCase();
    if (!EMAIL_REGEX.test(email)) {
      return safeAck(ack, { ok: false, error: 'EMAIL_DOMAIN' });
    }

    // 3. Names. firstName required, lastName optional. Both subject to the
    //    NAME_REGEX char allowlist and 40-char length cap. We trim before
    //    validating so leading/trailing whitespace isn't a footgun.
    const firstName = String((payload && payload.firstName) || '').trim();
    const lastName = String((payload && payload.lastName) || '').trim();

    if (!firstName) {
      return safeAck(ack, { ok: false, error: 'FIRST_NAME_REQUIRED' });
    }
    if (!isValidName(firstName)) {
      return safeAck(ack, { ok: false, error: 'INVALID_FIRST_NAME' });
    }
    if (lastName && !isValidName(lastName)) {
      return safeAck(ack, { ok: false, error: 'INVALID_LAST_NAME' });
    }

    // 4. Late-join rule: if past LOBBY, only EXISTING players are allowed
    //    back in (network drops / refreshes / browser restarts mid-game).
    //    Genuinely new players who didn't join during lobby are bounced.
    if (game.state !== STATE.LOBBY && !game.players.has(email)) {
      return safeAck(ack, { ok: false, error: 'GAME_STARTED' });
    }

    socket.data.email = email;
    // Persist with names. applyEvent's join case is idempotent on repeat
    // joins for the same email (and backfills names if absent on the
    // existing record — see applyEvent for the upgrade-path logic).
    recordAndPersist({ type: 'join', ts: Date.now(), email, firstName, lastName });

    safeAck(ack, { ok: true });

    // Snap this socket to the current game state — works for both lobby
    // joins and mid-game rejoins.
    sendCurrentStateTo(socket);

    // Refresh the lobby count for everyone (only meaningful in LOBBY state).
    if (game.state === STATE.LOBBY) {
      scheduleLobbyBroadcast();
    }
  });

  socket.on('player:answer', (payload, ack) => {
    if (socket.data.role !== 'player' || !socket.data.email) {
      return safeAck(ack, { ok: false, error: 'Not joined' });
    }
    if (game.state !== STATE.ACTIVE) {
      return safeAck(ack, { ok: false, error: 'No active question' });
    }
    const qIndex = payload && payload.qIndex;
    const option = payload && payload.option;
    if (qIndex !== game.currentIndex) {
      return safeAck(ack, { ok: false, error: 'Question changed' });
    }
    if (typeof option !== 'number' || option < 0 || option > 3) {
      return safeAck(ack, { ok: false, error: 'Invalid option' });
    }
    const player = game.players.get(socket.data.email);
    if (!player) return safeAck(ack, { ok: false, error: 'Unknown player' });
    if (player.answers.has(qIndex)) {
      return safeAck(ack, { ok: false, error: 'Already answered' });
    }

    recordAndPersist({
      type: 'answer',
      ts: Date.now(),
      email: socket.data.email,
      qIndex,
      option,
    });
    safeAck(ack, { ok: true });
    scheduleAnswerCountBroadcast();
  });

  // ----- Host events -----

  socket.on('host:advance', (_payload, ack) => {
    if (socket.data.role !== 'host') {
      return safeAck(ack, { ok: false, error: 'Not authorized' });
    }
    try {
      hostAdvance();
      safeAck(ack, { ok: true });
    } catch (e) {
      safeAck(ack, { ok: false, error: e.message });
    }
  });

  socket.on('host:reveal', (_payload, ack) => {
    if (socket.data.role !== 'host') {
      return safeAck(ack, { ok: false, error: 'Not authorized' });
    }
    try {
      hostReveal();
      safeAck(ack, { ok: true });
    } catch (e) {
      safeAck(ack, { ok: false, error: e.message });
    }
  });

  socket.on('host:reset', (_payload, ack) => {
    if (socket.data.role !== 'host') {
      return safeAck(ack, { ok: false, error: 'Not authorized' });
    }
    try {
      hostReset();
      console.log('host:reset performed; game wiped to lobby, code rotated to', game.roomCode);
      safeAck(ack, { ok: true });
    } catch (e) {
      safeAck(ack, { ok: false, error: e.message });
    }
  });
});

function safeAck(ack, payload) {
  if (typeof ack === 'function') ack(payload);
}

// =====================================================================
// CRASH RECOVERY
// =====================================================================

function replayFromDisk() {
  console.log(`Replaying state from ${STATE_FILE}...`);
  const startMs = Date.now();
  let count = 0;
  persistence.replay(STATE_FILE, (event) => {
    applyEvent(event);
    count++;
  });
  const elapsedMs = Date.now() - startMs;
  console.log(`Replayed ${count} events in ${elapsedMs}ms. ` +
    `state=${game.state}, players=${game.players.size}, currentIndex=${game.currentIndex}, ` +
    `roomCode=${game.roomCode || '(none yet)'}`);

  if (game.state === STATE.ACTIVE) {
    scheduleAutoClose();
  }
}

function ensureRoomCode() {
  if (game.roomCode) return;
  const code = generateRoomCode();
  recordAndPersist({ type: 'room_code_set', ts: Date.now(), code });
  console.log('Generated initial room code:', code);
}

// =====================================================================
// BOOT
// =====================================================================

persistence.init(STATE_FILE);
replayFromDisk();
ensureRoomCode();

httpServer.listen(PORT, () => {
  console.log(`Quiz server listening on :${PORT}`);
  console.log(`State file: ${STATE_FILE}`);
  console.log(`Allowed email domain: @${ALLOWED_EMAIL_DOMAIN}`);
  console.log(`Host token (first 4 chars): ${HOST_TOKEN.slice(0, 4)}...`);
  console.log(`Active room code: ${game.roomCode}`);
});

function shutdown(signal) {
  console.log(`Received ${signal}, shutting down...`);
  persistence.close(() => {
    httpServer.close(() => process.exit(0));
  });
  setTimeout(() => process.exit(1), 5000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));