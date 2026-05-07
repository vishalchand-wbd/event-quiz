# Load test

Stress-tests the live quiz server with simulated Socket.IO clients.

## Why a custom Node script (not Artillery / k6)

Artillery's `socketio` engine has known issues with Socket.IO 4.x — silent ack
drops and broken auth handshakes — making "passing" runs untrustworthy. k6
doesn't speak Socket.IO's protocol natively (Socket.IO is WebSocket plus a
framing/ack layer, not bare WebSocket). Both options would have us hand-rolling
or patching the framing protocol that `socket.io-client` already implements
correctly. So we use the same client library the browser uses.

## What it does

1. Connects N simulated player sockets at a configurable ramp rate.
2. Connects 1 host socket (with token) and 1 board observer socket.
3. Drives the full quiz: `host:advance` → players answer → `host:reveal` → next.
4. Each player picks an answer at a realistic delay — 70% in the last 3 seconds
   of the timer, 30% earlier — which mimics real human behavior and is the
   worst case for the throttled `state:answerCount` broadcaster.
5. Verifies two SLOs from the brief:
   - **Drop rate = 0** (every attempted answer accepted)
   - **Reveal latency < 3000ms** (host:reveal → state:reveal on board)
6. Exits 0 on PASS, 1 on FAIL, 2 on crash.

## Setup (on the VM)

```bash
cd /opt/quiz/tools
sudo -u quiz npm install --no-audit --no-fund
```

(Or copy the `tools/` directory anywhere with Node 20 and run `npm install`.)

## Run

You need the host token — read it from `/etc/quiz/quiz.env` (root only):

```bash
sudo grep HOST_TOKEN /etc/quiz/quiz.env
```

Then, **before** the test, wipe the state file so the game starts at lobby:

```bash
sudo systemctl stop quiz.service
sudo truncate -s 0 /var/lib/quiz/state.jsonl
sudo systemctl start quiz.service
```

Run the load test:

```bash
cd /opt/quiz/tools
node loadtest.js \
  --url http://localhost:3000 \
  --token "PASTE_HOST_TOKEN_HERE" \
  --domain example.com \
  --players 2500
```

(Substitute `--domain` with whatever you set as `ALLOWED_EMAIL_DOMAIN`.)

## Watching the server during the test

In a second SSH session:

```bash
# Real-time CPU + memory of the server process
top -p $(pgrep -f 'node /opt/quiz/server.js')

# Server logs
sudo journalctl -u quiz -f

# File descriptor count (should stay well under 8192)
sudo ls /proc/$(pgrep -f 'node /opt/quiz/server.js')/fd | wc -l
```

## Interpreting the output

Sample summary block:

```
Players joined:       2500 / 2500
Answers attempted:    7500
Answers succeeded:    7500
Answers rejected:     0
Drop rate:            0.000%
Ack latency p50:      4ms
Ack latency p95:      18ms
Ack latency p99:      42ms
Ack latency max:      89ms
Reveal latency max:   72ms

SLO: drop rate <= 0%       PASS
SLO: reveal latency < 3000ms  PASS
OVERALL: PASS
```

What to look at:

- **Drop rate** — must be 0%. Anything else means the server rejected (or
  failed to ack) an answer that should have been accepted. Check
  `reject reasons` in per-question output for the breakdown.
- **Ack latency p99** — should stay under ~200ms on a healthy run. If it spikes
  to 1000ms+, the server's event loop is queuing — consider either resizing
  the VM or reducing connection count.
- **Reveal latency** — should be tens of milliseconds typically, well under the
  3000ms SLO. If it exceeds 500ms, broadcast fan-out is bottlenecking.
- **answerCount events received** — should be roughly `4 × question_duration_s
  + 1`. For a 20s question that's about 80 events plus a final flush. If much
  fewer arrive, throttling is misbehaving.
- **arrival distribution** — the realistic-delay function targets ~70% in the
  last 3 seconds. Actual measured percentage should land within a few points
  of that.

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `EMFILE: too many open files` on load-gen side | Shell ulimit too low | `ulimit -n 8192` before running |
| `connect_error: Invalid host token` | Wrong/missing token | Re-read `/etc/quiz/quiz.env` |
| `Email must end in @...` from join acks | --domain mismatch | Match `ALLOWED_EMAIL_DOMAIN` exactly |
| Ack latency p99 > 1000ms | Server CPU saturated | `top` to confirm; resize to D2s_v5 |
| Host-side error "Cannot advance from state finished" | State file not wiped | Truncate `/var/lib/quiz/state.jsonl` |

## Headroom recommendation

The brief asks for 2000 concurrent. Run at **2500** for 25% headroom. If 2500
passes cleanly, you have margin for event-day surprises (slow phones, dropped
connections forcing reconnects, etc.).
