'use strict';

/**
 * Append-only JSON Lines persistence.
 *
 * One line = one JSON event. Events are durable as soon as they reach the
 * kernel write buffer, which survives a Node process crash (the requirement).
 * Surviving a full VM crash would additionally need fsync-per-write; we don't
 * do that here because (a) the brief only asks for process-crash recovery and
 * (b) fsync per write would block the event loop under 2000-answer bursts.
 */

const fs = require('fs');
const path = require('path');

let writeStream = null;

function init(filePath) {
  // Make sure the directory exists. /var/lib/quiz is created during deploy,
  // but this guard helps for local development too.
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  // createWriteStream with flag 'a' = append. Non-blocking; libuv queues writes
  // and preserves order. This is the right primitive for high-rate logging.
  writeStream = fs.createWriteStream(filePath, { flags: 'a' });

  writeStream.on('error', (err) => {
    // If the disk is full or the file becomes unwritable mid-event, we want to
    // crash loudly so systemd restarts us rather than silently lose answers.
    console.error('FATAL: state file write error:', err);
    process.exit(1);
  });
}

function append(event) {
  if (!writeStream) {
    throw new Error('persistence.init() must be called before append()');
  }
  // One JSON object per line. JSON.stringify is safe for our event shapes —
  // no functions, no circular refs, no BigInts.
  writeStream.write(JSON.stringify(event) + '\n');
}

/**
 * Read the state file from disk and apply each event in order.
 * Synchronous because we want replay to complete before the HTTP listener starts.
 */
function replay(filePath, onEvent) {
  if (!fs.existsSync(filePath)) {
    // First boot — nothing to replay.
    return;
  }

  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n');

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    try {
      const event = JSON.parse(line);
      onEvent(event);
    } catch (err) {
      // The realistic cause of a parse error is a truncated final line from a
      // crash mid-write. We log and skip; the state we lose is at most one
      // event, which the affected client can re-submit on reconnect.
      console.warn(`persistence.replay: skipping unparseable line ${i + 1}: ${err.message}`);
    }
  }
}

function close(cb) {
  if (writeStream) {
    writeStream.end(cb);
  } else if (cb) {
    cb();
  }
}

module.exports = { init, append, replay, close };
