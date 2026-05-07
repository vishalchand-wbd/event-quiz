# Live Quiz Server

Real-time multiplayer quiz on a single Node 20 VM. Socket.IO, Express, JSONL
persistence, systemd auto-restart, Caddy reverse proxy with auto-HTTPS.

## Layout

```
quiz/
├── package.json
├── server.js              # main process — HTTP, Socket.IO, state machine, scoring
├── persistence.js         # append-only JSONL writer + boot replay
├── quiz.json              # questions (edit by hand)
├── public/                # static frontends (placeholders for Stage 2; real ones in Stage 3)
│   ├── play.html
│   ├── host.html
│   └── board.html
└── deploy/
    ├── quiz.service       # systemd unit
    ├── Caddyfile          # reverse proxy + auto HTTPS
    └── install.sh         # one-shot deploy script
```

## Deploy

From your **Windows laptop** (PowerShell):

```powershell
# Push the project to your home dir on the VM
scp -r .\quiz azureuser@test-quiz.duckdns.org:~/
```

Then SSH in and run the installer:

```bash
ssh azureuser@test-quiz.duckdns.org
cd ~/quiz
bash deploy/install.sh
```

The installer will prompt for the allowed email domain, generate a random
host token, and start everything. **Save the host token** — the moderator
will use it to access `/host`.

## Verify

```bash
sudo systemctl status quiz.service     # expect "active (running)"
sudo journalctl -u quiz -f             # watch logs in real time

curl -sI https://test-quiz.duckdns.org/health   # expect HTTP/2 200
curl -s  https://test-quiz.duckdns.org/health   # expect JSON: state, playerCount, etc.
```

In a browser, hit:
- `https://test-quiz.duckdns.org/play`  → /play placeholder
- `https://test-quiz.duckdns.org/host`  → /host placeholder
- `https://test-quiz.duckdns.org/board` → /board placeholder

All three should show their respective placeholder page over HTTPS with a
green padlock (Caddy fetches the Let's Encrypt cert on first config reload).

## Operational reference

- **Logs:** `sudo journalctl -u quiz -f`
- **State file:** `/var/lib/quiz/state.jsonl` — append-only, replayed on boot
- **Restart:** `sudo systemctl restart quiz.service`
- **Edit questions:** edit `/opt/quiz/quiz.json`, then `sudo systemctl restart quiz.service`
- **Wipe state for a fresh game:** `sudo systemctl stop quiz.service && sudo truncate -s 0 /var/lib/quiz/state.jsonl && sudo systemctl start quiz.service`
- **Env vars:** `/etc/quiz/quiz.env` (mode 0600, root-owned)
