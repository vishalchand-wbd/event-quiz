#!/usr/bin/env bash
# One-shot deployment helper. Run from the project root on the VM.
#
# Usage:
#   cd ~/quiz
#   bash deploy/install.sh

set -euo pipefail

if [[ "$EUID" -eq 0 ]]; then
  echo "Run as a normal user (not root). The script uses sudo where needed."
  exit 1
fi

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
echo "Deploying from: $PROJECT_DIR"

echo "==> Creating quiz system user (if missing)"
if ! id quiz &>/dev/null; then
  sudo useradd --system --shell /usr/sbin/nologin --home /opt/quiz quiz
fi

echo "==> Creating directories"
sudo mkdir -p /opt/quiz /var/lib/quiz /etc/quiz /var/log/caddy

echo "==> Copying project files to /opt/quiz"
sudo rsync -a --delete \
  --exclude node_modules \
  --exclude '*.jsonl' \
  "$PROJECT_DIR/" /opt/quiz/
sudo chown -R quiz:quiz /opt/quiz /var/lib/quiz

echo "==> Installing npm dependencies (production only)"
cd /opt/quiz
sudo -u quiz npm install --omit=dev --no-audit --no-fund

echo "==> Configuring environment file"
if [[ ! -f /etc/quiz/quiz.env ]]; then
  HOST_TOKEN_VALUE=$(openssl rand -hex 24)
  read -r -p "Allowed email domain (e.g. example.com): " EMAIL_DOMAIN
  sudo tee /etc/quiz/quiz.env > /dev/null <<EOF
HOST_TOKEN=$HOST_TOKEN_VALUE
ALLOWED_EMAIL_DOMAIN=$EMAIL_DOMAIN
EOF
  sudo chmod 600 /etc/quiz/quiz.env
  sudo chown root:root /etc/quiz/quiz.env
  echo
  echo "==> Generated host token (SAVE THIS — moderator needs it):"
  echo "    $HOST_TOKEN_VALUE"
  echo
else
  echo "/etc/quiz/quiz.env already exists, leaving as-is."
fi

echo "==> Installing systemd unit"
sudo cp /opt/quiz/deploy/quiz.service /etc/systemd/system/quiz.service
sudo systemctl daemon-reload
sudo systemctl enable quiz.service
sudo systemctl restart quiz.service

echo "==> Configuring Caddy"
sudo cp /opt/quiz/deploy/Caddyfile /etc/caddy/Caddyfile
sudo systemctl reload caddy

echo
echo "==> Done."
echo "    Verify with:"
echo "      sudo systemctl status quiz.service"
echo "      sudo journalctl -u quiz -f"
echo "      curl -sI https://quizs.live/health"
