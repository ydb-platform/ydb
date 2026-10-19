#!/bin/bash
# Register this host as a GitHub Actions runner. Invoked as root over SSH.
# Env: REPO_URL, GITHUB_TOKEN, REMOVE_TOKEN, RUNNER_NAME, RUNNER_LABELS, PRESET_NAME
set -euo pipefail

: "${REPO_URL:?}"
: "${GITHUB_TOKEN:?}"
: "${REMOVE_TOKEN:?}"
: "${RUNNER_NAME:?}"
: "${RUNNER_LABELS:?}"

RUNNER_USERNAME=runner
H=/home/$RUNNER_USERNAME
PRESET_NAME="${PRESET_NAME:-}"

instance_id=$(curl -sS --retry 8 -H Metadata-Flavor:Google \
  169.254.169.254/computeMetadata/v1/instance/vendor/identity/document | jq -r '.instanceId')

mkdir -p "$H/actions_runner"
cd "$H/actions_runner"

# Image cache is older than the GitHub registration minimum (2.329.0).
RUNNER_VERSION=2.338.0
current_version=""
if [ -x ./config.sh ]; then
  current_version=$(sudo -u "$RUNNER_USERNAME" ./config.sh --version 2>/dev/null | awk 'NR==1{print $1}' || true)
fi
if [ "$current_version" != "$RUNNER_VERSION" ]; then
  tmp=$(mktemp -d)
  if [ -n "${AGENT_MIRROR_URL_PREFIX:-}" ]; then
    agent_latest_version=$(curl -fsS --retry 8 "$AGENT_MIRROR_URL_PREFIX/latest" || true)
    if [ -n "$agent_latest_version" ]; then
      curl -fsS --retry 8 -o "$tmp/runner.tgz" "$AGENT_MIRROR_URL_PREFIX/$agent_latest_version" || true
    fi
  fi
  if [ ! -s "$tmp/runner.tgz" ]; then
    curl -fsSL --retry 8 -o "$tmp/runner.tgz" \
      "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
  fi
  find . -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  tar -xzf "$tmp/runner.tgz" -C "$H/actions_runner"
  rm -rf "$tmp"
  chown -R "$RUNNER_USERNAME:$RUNNER_USERNAME" "$H/actions_runner"
fi

if [ -f .runner ]; then
  if [ -f .service ]; then
    systemctl stop "$(cat .service)" || true
  fi
  sudo -u "$RUNNER_USERNAME" ./config.sh remove --unattended --token "$REMOVE_TOKEN"
fi

sudo -u "$RUNNER_USERNAME" ./config.sh --unattended --disableupdate --replace \
  --url "$REPO_URL" \
  --token "$GITHUB_TOKEN" \
  --name "$RUNNER_NAME" \
  --labels "${RUNNER_LABELS},instance:${instance_id}"

if [ -f .service ] && ! systemctl cat "$(cat .service)" >/dev/null 2>&1; then
  rm -f .service
fi
if [ ! -f .service ]; then
  ./svc.sh install "$RUNNER_USERNAME"
fi
SVC_NAME=$(cat .service)

mkdir -p "/etc/systemd/system/${SVC_NAME}.d"
cat > "/etc/systemd/system/${SVC_NAME}.d/restart.conf" <<EOF
[Service]
Restart=always
RestartSec=10
EOF

systemctl daemon-reload
systemctl enable "$SVC_NAME"
systemctl restart "$SVC_NAME"

cat > /etc/default/unified_agent <<EOF
GH_RUNNER_NAME=${RUNNER_NAME}
GH_PRESET_NAME=${PRESET_NAME}
EOF
if systemctl cat unified-agent.service >/dev/null 2>&1; then
  systemctl restart unified-agent.service || true
fi

systemctl is-active "$SVC_NAME"
