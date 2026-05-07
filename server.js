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
 *   ANY state --host:reset--> lobby (clears all players, scores, timers)
 *
 * Ten Socket.IO events:
 *   Client → Server: player:join, player:answer, host:advance, host:reveal, host:reset
 *   Server → Client: state:lobby, state:question, state:answerCount,
 *                    state:reveal, state:finished, state:reset, error
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
  currentIndex: -1,           // index of the current question (-1 = no question yet)
  currentStartedAt: null,     // server timestamp (ms) when current question started
  currentEndsAt: null,        // server timestamp (ms) when current question's timer expires
  players: new Map(),         // email -> { email, joinedAt, score, answers: Map<qIndex, record> }
  currentAnswers: new Map(),  // email -> answer record, JUST for the current question
  closeTimer: null,           // setTimeout handle for auto-close on timer expiry
};

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
//   Two correct answers 50ms apart yield identical scores most of the time;
//   the email comparator guarantees a stable ranking across replays.
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
//
// applyEvent() mutates state. It runs both at runtime (after we accept a
// new event) AND during boot replay (when reading events back from disk).
// It must therefore be:
//   - deterministic given the event payload
//   - idempotent where it matters (e.g. duplicate "answer" events are no-ops)
//   - free of side effects (no I/O, no broadcasts) — those happen separately
// =====================================================================

function applyEvent(event) {
  switch (event.type) {
    case 'join': {
      // Players are keyed by email. Re-joining = no-op (keeps existing score).
      if (!game.players.has(event.email)) {
        game.players.set(event.email, {
          email: event.email,
          joinedAt: event.ts,
          score: 0,
          answers: new Map(),
        });
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

      // currentAnswers tracks ONLY the active question (cleared on each
      // question_started). During replay this works because events arrive in
      // order; the question_started for index N is applied before answers for N.
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
      // Wipe ALL logical game state back to a freshly-booted lobby.
      // Note: closeTimer is a runtime-only handle, NOT logical state — it must
      // be cleared by the runtime caller (hostReset) before recordAndPersist,
      // not here, so applyEvent stays free of side effects (rule for replay).
      game.state = STATE.LOBBY;
      game.currentIndex = -1;
      game.currentStartedAt = null;
      game.currentEndsAt = null;
      game.players = new Map();
      game.currentAnswers = new Map();
      break;
    }

    default:
      console.warn('applyEvent: unknown event type:', event.type);
  }
}

// =====================================================================
// RECORD-AND-PERSIST helper
//
// Wrap up the "right way" to make a state change: build the event, apply it,
// then append it to disk. Anyone calling this can be sure nothing diverges.
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
    serverNow: Date.now(),     // lets clients estimate clock skew for the local timer
    // NOTE: correctOption is NOT included here — clients learn it on reveal.
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
  // If caller passed pre-computed values (broadcast hot path), use them.
  // Otherwise compute fresh — single-socket paths (e.g. sendCurrentStateTo on
  // a late reconnect) take this branch and pay the cost just for themselves.
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
      // /board and /host get the public reveal; each /play client gets a personalized one.
      const publicReveal = buildRevealPayload();
      // Precompute the rank lookup once. Without this, each per-player payload
      // re-sorted all N players (O(N²) — at 2500 players, ~4 seconds of CPU).
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
  // Used when a client just connected (or just joined as a player) — they need
  // to know whatever state the game is in right now.
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
// THROTTLED ANSWER COUNT BROADCASTS (max 4/sec — spec)
//
// While a question is active, every accepted answer schedules a broadcast.
// If a broadcast is already pending, we coalesce — that's the throttle.
// =====================================================================

let answerCountTimer = null;

function scheduleAnswerCountBroadcast() {
  if (answerCountTimer) return;     // already pending; will fire soon
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
//
// Same pattern. A naive io.emit on every join is O(N²) during ramp — at 2500
// joins, that's ~3M total socket writes (1+2+...+2500). The throttle caps it
// at 4 broadcasts/sec regardless of join rate.
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
    // After replay, the question's timer may already have expired during downtime.
    // Close immediately so we resume in the right state.
    closeCurrentQuestion();
  } else {
    game.closeTimer = setTimeout(closeCurrentQuestion, remainingMs);
  }
}

function closeCurrentQuestion() {
  if (game.state !== STATE.ACTIVE) return;     // already closed by host or by an earlier path
  recordAndPersist({
    type: 'question_closed',
    ts: Date.now(),
    index: game.currentIndex,
  });
  // Final answer-count flush so /host and /board see the true total before reveal.
  io.emit('state:answerCount', {
    count: game.currentAnswers.size,
    total: game.players.size,
    index: game.currentIndex,
    final: true,
  });
  // We do NOT auto-reveal — the moderator decides when via host:reveal.
}

// =====================================================================
// HOST ACTIONS
// =====================================================================

function hostAdvance() {
  // Valid from LOBBY (start the first question) or REVEALED (next question, or finish).
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
  // Valid from ACTIVE (early reveal) or CLOSED (normal flow).
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
  // Allowed from ANY state. The host page guards against accidental clicks
  // with a confirmation dialog; server trusts the moderator beyond that.

  // 1. Cancel any pending runtime side effects so they don't fire against the
  //    fresh-lobby state we're about to install.
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

  // 2. Persist + apply the reset event. After this returns, game.players is
  //    empty, currentIndex is -1, and game.state is LOBBY.
  recordAndPersist({ type: 'reset', ts: Date.now() });

  // 3. Clear cached identity from every connected player socket. Without this,
  //    a player who misses the state:reset event on the wire could still pass
  //    the `socket.data.email` guard in player:answer and produce an "Unknown
  //    player" error the client can't recover from cleanly.
  for (const [, sock] of io.of('/').sockets) {
    if (sock.data.role === 'player') {
      sock.data.email = null;
    }
  }

  // 4. Tell every connected client to wipe local UI state. The follow-up
  //    state:lobby broadcast (from broadcastState) gives them the new state.
  io.emit('state:reset', {});
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
  currentIndex: game.currentIndex,
  playerCount: game.players.size,
  uptimeSec: Math.round(process.uptime()),
}));

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  // WebSocket only — long-polling fallback doubles HTTP overhead at scale.
  transports: ['websocket'],
  // Slightly relaxed pings — cuts overhead at 2000 connections.
  pingInterval: 25000,
  pingTimeout: 20000,
  // Allow larger handshake payloads if needed.
  maxHttpBufferSize: 1e6,
});

// =====================================================================
// EMAIL VALIDATION
// =====================================================================

// Build the regex once. Escape dots in the domain so "wbd.com" isn't read
// as "wbd<any-char>com" by the regex engine.
const escapedDomain = ALLOWED_EMAIL_DOMAIN.replace(/\./g, '\\.');
const EMAIL_REGEX = new RegExp(`^[A-Za-z0-9._%+-]+@${escapedDomain}$`, 'i');

// =====================================================================
// SOCKET HANDLERS
// =====================================================================

io.on('connection', (socket) => {
  // The client sends auth info during the handshake. Three roles:
  //   - 'host'   : moderator; must present HOST_TOKEN
  //   - 'board'  : projector display; no auth (read-only, no PII)
  //   - 'player' : default; identifies later via player:join + email
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
    // Player must call player:join before they're considered "in".
  }

  // ----- Player events -----

  socket.on('player:join', (payload, ack) => {
    if (socket.data.role !== 'player') {
      return safeAck(ack, { ok: false, error: 'Not a player socket' });
    }
    const email = String((payload && payload.email) || '').trim().toLowerCase();
    if (!EMAIL_REGEX.test(email)) {
      return safeAck(ack, { ok: false, error: `Email must end in @${ALLOWED_EMAIL_DOMAIN}` });
    }
    socket.data.email = email;
    // Persist the join (idempotent — duplicate joins for same email are no-ops in applyEvent).
    recordAndPersist({ type: 'join', ts: Date.now(), email });

    safeAck(ack, { ok: true });

    // Snap this socket to the current game state.
    sendCurrentStateTo(socket);
    // Refresh the lobby count for everyone (only meaningful in LOBBY state).
    // Throttled so a 2000-player ramp doesn't trigger O(N²) broadcast traffic.
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

    // Server timestamp — we never trust client time.
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
      console.log('host:reset performed; game wiped to lobby');
      safeAck(ack, { ok: true });
    } catch (e) {
      safeAck(ack, { ok: false, error: e.message });
    }
  });
});

// Tiny helper so we don't crash if a buggy client omits the ack callback.
function safeAck(ack, payload) {
  if (typeof ack === 'function') ack(payload);
}

// =====================================================================
// CRASH RECOVERY: replay events from JSONL on boot
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
    `state=${game.state}, players=${game.players.size}, currentIndex=${game.currentIndex}`);

  // If we landed in ACTIVE state, the timer may already have expired during
  // downtime — scheduleAutoClose handles both "expired" and "still ticking" cases.
  if (game.state === STATE.ACTIVE) {
    scheduleAutoClose();
  }
}

// =====================================================================
// BOOT
// =====================================================================

persistence.init(STATE_FILE);
replayFromDisk();

httpServer.listen(PORT, () => {
  console.log(`Quiz server listening on :${PORT}`);
  console.log(`State file: ${STATE_FILE}`);
  console.log(`Allowed email domain: @${ALLOWED_EMAIL_DOMAIN}`);
  console.log(`Host token (first 4 chars): ${HOST_TOKEN.slice(0, 4)}...`);
});

// Graceful shutdown — flush the write stream before exit so we don't lose the
// last few buffered events when systemd sends SIGTERM during a deploy.
function shutdown(signal) {
  console.log(`Received ${signal}, shutting down...`);
  persistence.close(() => {
    httpServer.close(() => process.exit(0));
  });
  setTimeout(() => process.exit(1), 5000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
