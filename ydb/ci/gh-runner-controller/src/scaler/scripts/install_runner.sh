#!/usr/bin/env bash
set -e

source /install_runner.env

set -x

function fail() {
	echo $1
	exit 1
}

function get_instance_id {
  curl -sS --retry 8 -H Metadata-Flavor:Google 169.254.169.254/computeMetadata/v1/instance/vendor/identity/document | jq -r '.instanceId'
}

vars_to_check=("REPO_URL" "GITHUB_TOKEN" "RUNNER_NAME" "RUNNER_LABELS" "RUNNER_USERNAME")

for var_name in "${vars_to_check[@]}"; do
  if [ -z "${!var_name}" ]; then
    fail "${var_name} undefined"
  fi
done


H=/home/$RUNNER_USERNAME
mkdir "$H"/actions_runner
cd "$_"

{
  agent_latest_version=$(curl -sS --retry 8 "$AGENT_MIRROR_URL_PREFIX/latest") && \
  agent_download_url="$AGENT_MIRROR_URL_PREFIX/$agent_latest_version" && \
  curl -sS --retry 8 "$agent_download_url" | tar -xz
} || {
  # use bundled agent
  cp -rT /opt/cache/actions-runner/latest/ "$H"/actions_runner
}

chown -R "$RUNNER_USERNAME":"$RUNNER_USERNAME" "$H"/actions_runner

instance_id=$(get_instance_id)

sudo -u "$RUNNER_USERNAME" ./config.sh --unattended --disableupdate --url "${REPO_URL}" --token "${GITHUB_TOKEN}" --name "${RUNNER_NAME}" --labels "${RUNNER_LABELS},instance:${instance_id}"

./svc.sh install "${RUNNER_USERNAME}" || fail "failed to install service"

SVC_NAME=$(cat .service)

systemctl daemon-reload || fail "failed to reload systemd"
systemctl enable $SVC_NAME
systemctl start $SVC_NAME

echo "GH_RUNNER_NAME=${RUNNER_NAME}"> /etc/default/unified_agent
systemctl restart unified-agent.service
