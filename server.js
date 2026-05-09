'use strict';

/**
 * Real-time quiz server.
 *
 * One Node process, in-memory state, JSONL persistence for crash recovery.
 *
 * State machine (post Stage D — finished + podium reveal):
 *   lobby --advance--> preview --advance--> active --(timer expires)--> closed
 *                                                  \                       |
 *                                                   \--(early reveal)--/   |
 *                                                                          |
 *   closed --reveal--> revealed --advance--> preview(next) OR finished
 *
 *   finished --host:revealPodium--> finished + podiumRevealed=true
 *                                   (sub-flag, NOT a new state — game is
 *                                    semantically still FINISHED)
 *
 *   ANY state --host:reset--> lobby (clears all players, scores, timers,
 *                                    rotates the room code, clears the
 *                                    podiumRevealed flag)
 *
 * Sixteen Socket.IO events:
 *   Client → Server: player:join, player:answer,
 *                    host:advance, host:reveal, host:revealPodium, host:reset
 *   Server → Client: state:lobby, state:question_preview, state:question,
 *                    state:answerCount, state:reveal, state:finished,
 *                    state:podiumRevealed, state:reset, state:meta, error
 *
 * Two-phase question display (Stage E):
 *   - host:advance from LOBBY/REVEALED → PREVIEW (text shown, no timer)
 *   - host:advance from PREVIEW → ACTIVE (timer starts, options revealed)
 *   - state:question_preview is role-aware: /board sees text, /host sees
 *     text+options, /play sees ONLY {index, total} (player looks at projector)
 *
 * Podium reveal flow (Stage D):
 *   - When state enters FINISHED, /board shows top 10 with top 3 names blurred
 *     plus a "Thanks for participating" message.
 *   - Host clicks "Reveal Podium" → state:podiumRevealed broadcasts to all
 *     clients → /board animates list-to-pillars transformation with confetti.
 *   - state:finished payload includes top10 (renamed from podium, bumped from
 *     5 to 10 entries) AND a podiumRevealed boolean. Reconnects mid-game-
 *     with-revealed-podium get the post-celebration UI without replaying the
 *     animation.
 *
 * Room code:
 *   - 4-digit numeric code generated once at boot (or restored from JSONL replay)
 *   - Required on player:join; mismatch → INVALID_CODE
 *   - Rotated on host:reset so leaked codes don't carry over to next game
 *   - Broadcast via state:meta on connect and after reset
 *
 * Late-join rules:
 *   - NEW players joining when state != LOBBY → rejected with GAME_STARTED
 *     (PREVIEW counts as past-lobby — game has effectively started)
 *   - EXISTING players (already in game.players from lobby) → allowed to rejoin
 *     from any state. They get snapped to the current state.
 *
 * Player identity:
 *   - Email is the canonical identity (lookups, idempotency, late-join check).
 *   - firstName (required) and lastName (optional) stored alongside but
 *     display-only — the UI shows names instead of email everywhere.
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
// CONFIGURATION
// =====================================================================

const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST_TOKEN = process.env.HOST_TOKEN || '';
const ALLOWED_EMAIL_DOMAIN = process.env.ALLOWED_EMAIL_DOMAIN || '';
const STATE_FILE = process.env.STATE_FILE || '/var/lib/quiz/state.jsonl';
const QUIZ_FILE = process.env.QUIZ_FILE || path.join(__dirname, 'quiz.json');
const PUBLIC_DIR = path.join(__dirname, 'public');

// Theme selection. Set THEME=clean or THEME=funky in /etc/quiz/quiz.env to flip.
// Default = funky (current as of Stage F). Both themes share server.js, the
// wire contract, and persistence; only the HTML/CSS shipped to clients differs.
const VALID_THEMES = ['clean', 'funky'];
const THEME = (process.env.THEME || 'funky').toLowerCase();
const VIEWS_DIR = path.join(__dirname, 'views', THEME);

const ANSWER_COUNT_THROTTLE_MS = 250;

if (!HOST_TOKEN) {
  console.error('FATAL: HOST_TOKEN environment variable not set.');
  process.exit(1);
}
if (!ALLOWED_EMAIL_DOMAIN) {
  console.error('FATAL: ALLOWED_EMAIL_DOMAIN environment variable not set.');
  process.exit(1);
}
if (!VALID_THEMES.includes(THEME)) {
  console.error(`FATAL: THEME="${THEME}" is invalid. Must be one of: ${VALID_THEMES.join(', ')}.`);
  process.exit(1);
}
if (!fs.existsSync(VIEWS_DIR)) {
  console.error(`FATAL: Theme directory not found: ${VIEWS_DIR}`);
  process.exit(1);
}

// =====================================================================
// QUIZ DATA
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
  PREVIEW: 'preview',
  ACTIVE: 'active',
  CLOSED: 'closed',
  REVEALED: 'revealed',
  FINISHED: 'finished',
});

const game = {
  state: STATE.LOBBY,
  roomCode: null,
  currentIndex: -1,
  currentStartedAt: null,
  currentEndsAt: null,
  players: new Map(),         // email -> { email, firstName, lastName, joinedAt, score, answers: Map }
  currentAnswers: new Map(),  // email -> answer record, JUST for the current question
  closeTimer: null,
  // Stage D: sub-flag of FINISHED. False at game start; flipped true by host's
  // Reveal Podium click. Cleared on reset. Persisted via 'podium_revealed'
  // event so server reboots in mid-finished-with-revealed-podium correctly.
  podiumRevealed: false,
};

// =====================================================================
// ROOM CODE
// =====================================================================

function generateRoomCode() {
  return String(1000 + Math.floor(Math.random() * 9000));
}

// =====================================================================
// SCORING
// =====================================================================

function calculateScore(correct, elapsedMs, durationMs) {
  if (!correct) return 0;
  const raw = 1000 - Math.floor((elapsedMs / durationMs) * 500);
  return Math.max(500, raw);
}

// =====================================================================
// LEADERBOARD
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

    case 'question_previewed': {
      game.state = STATE.PREVIEW;
      game.currentIndex = event.index;
      game.currentStartedAt = null;
      game.currentEndsAt = null;
      game.currentAnswers = new Map();
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
      if (!player) break;
      if (player.answers.has(event.qIndex)) break;

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
      // podiumRevealed stays false on entering FINISHED; flipped only by an
      // explicit 'podium_revealed' event later. We do NOT reset it to false
      // here in case a replay sequence is finished→podium_revealed (the flag
      // would get clobbered). On a clean game start, the flag is already
      // false from boot or from the previous reset.
      break;
    }

    case 'podium_revealed': {
      // Stage D: host triggered the celebration. Idempotent on replay.
      game.podiumRevealed = true;
      break;
    }

    case 'reset': {
      game.state = STATE.LOBBY;
      game.currentIndex = -1;
      game.currentStartedAt = null;
      game.currentEndsAt = null;
      game.players = new Map();
      game.currentAnswers = new Map();
      // Stage D: clear podium reveal flag so the next game starts blurred.
      game.podiumRevealed = false;
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

function recordAndPersist(event) {
  applyEvent(event);
  persistence.append(event);
}

// =====================================================================
// PAYLOAD BUILDERS
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

function buildQuestionPreviewPayload(role) {
  const q = quiz.questions[game.currentIndex];
  const base = {
    index: game.currentIndex,
    total: quiz.questions.length,
  };
  if (role === 'board' || role === 'host') {
    base.text = q.text;
  }
  if (role === 'host') {
    base.options = q.options;
  }
  return base;
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
    leaderboard: getLeaderboard(10),
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

// Stage D: state:finished payload. Renamed `podium` → `top10` and bumped from
// top 5 to top 10. Includes the podiumRevealed flag so reconnecting clients
// render the right phase (blurred list vs revealed pillars) without animation.
function buildFinishedPayload() {
  return {
    top10: getLeaderboard(10),
    podiumRevealed: game.podiumRevealed,
  };
}

// =====================================================================
// BROADCASTS
// =====================================================================

function broadcastState() {
  switch (game.state) {
    case STATE.LOBBY:
      io.emit('state:lobby', { playerCount: game.players.size });
      break;

    case STATE.PREVIEW: {
      io.to('board').emit('state:question_preview', buildQuestionPreviewPayload('board'));
      io.to('host').emit('state:question_preview', buildQuestionPreviewPayload('host'));
      const playerPayload = buildQuestionPreviewPayload('player');
      for (const [, sock] of io.of('/').sockets) {
        if (sock.data.role === 'player' && sock.data.email) {
          sock.emit('state:question_preview', playerPayload);
        }
      }
      break;
    }

    case STATE.ACTIVE:
    case STATE.CLOSED:
      io.emit('state:question', buildQuestionPayload());
      break;

    case STATE.REVEALED: {
      const publicReveal = buildRevealPayload();
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
      io.emit('state:finished', buildFinishedPayload());
      break;
  }
}

function sendCurrentStateTo(socket) {
  socket.emit('state:meta', { code: game.roomCode });

  switch (game.state) {
    case STATE.LOBBY:
      socket.emit('state:lobby', { playerCount: game.players.size });
      break;
    case STATE.PREVIEW:
      socket.emit('state:question_preview', buildQuestionPreviewPayload(socket.data.role));
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
      // The flag in the payload tells the client which phase to render.
      // No state:podiumRevealed sent — that's only for live transitions.
      socket.emit('state:finished', buildFinishedPayload());
      break;
  }
}

// =====================================================================
// THROTTLED BROADCASTS
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
  if (game.state === STATE.LOBBY || game.state === STATE.REVEALED) {
    const nextIndex = game.currentIndex + 1;
    if (nextIndex >= quiz.questions.length) {
      recordAndPersist({ type: 'finished', ts: Date.now() });
      broadcastState();
      return;
    }
    recordAndPersist({ type: 'question_previewed', ts: Date.now(), index: nextIndex });
    broadcastState();
    return;
  }

  if (game.state === STATE.PREVIEW) {
    recordAndPersist({
      type: 'question_started',
      ts: Date.now(),
      index: game.currentIndex,
    });
    scheduleAutoClose();
    broadcastState();
    return;
  }

  throw new Error(`Cannot advance from state "${game.state}"`);
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

// Stage D: host triggers the podium celebration. Only valid in FINISHED state.
// Idempotent — duplicate clicks are no-ops (don't error, don't re-broadcast).
function hostRevealPodium() {
  if (game.state !== STATE.FINISHED) {
    throw new Error(`Cannot reveal podium from state "${game.state}"`);
  }
  if (game.podiumRevealed) {
    // Already revealed; no-op. This makes the action safe to retry from the
    // host UI without worrying about replaying the celebration.
    return;
  }
  recordAndPersist({ type: 'podium_revealed', ts: Date.now() });
  // Animation trigger only — clients already have the top10 data from
  // state:finished. No payload needed.
  io.emit('state:podiumRevealed', {});
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

  const newCode = generateRoomCode();
  recordAndPersist({ type: 'room_code_set', ts: Date.now(), code: newCode });
  console.log('host:reset: new room code is', newCode);

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
app.get('/play', (_req, res) => res.sendFile(path.join(VIEWS_DIR, 'play.html')));
app.get('/host', (_req, res) => res.sendFile(path.join(VIEWS_DIR, 'host.html')));
app.get('/board', (_req, res) => res.sendFile(path.join(VIEWS_DIR, 'board.html')));
app.get('/health', (_req, res) => res.json({
  status: 'ok',
  state: game.state,
  podiumRevealed: game.podiumRevealed,
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
    socket.emit('state:meta', { code: game.roomCode });
  }

  // ----- Player events -----

  socket.on('player:join', (payload, ack) => {
    if (socket.data.role !== 'player') {
      return safeAck(ack, { ok: false, error: 'Not a player socket' });
    }

    const submittedCode = String((payload && payload.code) || '').trim();
    if (!submittedCode || submittedCode !== game.roomCode) {
      return safeAck(ack, { ok: false, error: 'INVALID_CODE' });
    }

    const email = String((payload && payload.email) || '').trim().toLowerCase();
    if (!EMAIL_REGEX.test(email)) {
      return safeAck(ack, { ok: false, error: 'EMAIL_DOMAIN' });
    }

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

    if (game.state !== STATE.LOBBY && !game.players.has(email)) {
      return safeAck(ack, { ok: false, error: 'GAME_STARTED' });
    }

    socket.data.email = email;
    recordAndPersist({ type: 'join', ts: Date.now(), email, firstName, lastName });

    safeAck(ack, { ok: true });

    sendCurrentStateTo(socket);

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

  // Stage D: new host action. Triggers the podium celebration on /board.
  socket.on('host:revealPodium', (_payload, ack) => {
    if (socket.data.role !== 'host') {
      return safeAck(ack, { ok: false, error: 'Not authorized' });
    }
    try {
      hostRevealPodium();
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
    `podiumRevealed=${game.podiumRevealed}, roomCode=${game.roomCode || '(none yet)'}`);

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
  console.log(`Active theme: ${THEME} (views from ${VIEWS_DIR})`);
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