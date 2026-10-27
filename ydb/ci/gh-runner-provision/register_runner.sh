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
  169.254.169.254/computeMetadata/v1/instance/vendor/identity/document | jq -r '.instanceId // empty')
if [ -z "$instance_id" ] || [ "$instance_id" = "null" ]; then
  instance_id=""
fi

mkdir -p "$H/actions_runner"
cd "$H/actions_runner"

# Image cache is older than the GitHub registration minimum (2.329.0).
RUNNER_VERSION=2.338.0
current_version=""
if [ -x ./config.sh ]; then
  current_version=$(sudo -u "$RUNNER_USERNAME" ./config.sh --version 2>/dev/null | awk 'NR==1{print $1}' || true)
fi
stop_runner() {
  # stdin is the rest of this script. uninstall and remove must not consume it.
  if [ -f .service ]; then
    ./svc.sh uninstall </dev/null || true
  fi
  if [ -f .runner ]; then
    sudo -u "$RUNNER_USERNAME" ./config.sh remove --token "$REMOVE_TOKEN" </dev/null
  fi
}

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
  stop_runner
  find . -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  tar -xzf "$tmp/runner.tgz" -C "$H/actions_runner"
  rm -rf "$tmp"
  chown -R "$RUNNER_USERNAME:$RUNNER_USERNAME" "$H/actions_runner"
fi

if [ -f .runner ]; then
  stop_runner
fi

labels="$RUNNER_LABELS"
if [ -n "$instance_id" ]; then
  labels="${RUNNER_LABELS},instance:${instance_id}"
fi
sudo -u "$RUNNER_USERNAME" ./config.sh --unattended --disableupdate --replace \
  --url "$REPO_URL" \
  --token "$GITHUB_TOKEN" \
  --name "$RUNNER_NAME" \
  --labels "$labels"

if [ -f .service ]; then
  svc=$(tr -d '[:space:]' < .service)
  case "$svc" in
    actions.runner.*.service)
      if ! systemctl cat "$svc" >/dev/null 2>&1; then
        rm -f .service
      fi
      ;;
    *)
      rm -f .service
      ;;
  esac
fi
if [ ! -f .service ]; then
  ./svc.sh install "$RUNNER_USERNAME"
fi
SVC_NAME=$(tr -d '[:space:]' < .service)
case "$SVC_NAME" in
  actions.runner.*.service) ;;
  *) echo "bad runner service name: $SVC_NAME" >&2; exit 1 ;;
esac

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
