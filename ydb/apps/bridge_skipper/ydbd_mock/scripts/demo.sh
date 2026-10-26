#!/usr/bin/env bash
# Run the unmodified bridge skipper demo against a running ydbd_mock cluster.
#
#   demo.sh [--fresh] [-s STATE_FILE] [-e LABEL] [demo args...]
#
# --fresh   delete the state file first (default state file: $RUN/skipper_state.json)
# -e        label of any mock node used to resolve the topology (default mock-pile1-1)
#
# The demo is started with --ydb <ydb_shim.py>, so every CLI call is checked and
# rewritten to a mock endpoint. The demo's sources are never touched, and no
# .pyc files are written next to them.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../../.." && pwd)"
WORK="${YDBD_MOCK_WORK:-$HOME/ydbd_mock_work}"
RUN="${YDBD_MOCK_RUN_DIR:-$WORK/run}"

STATE="$RUN/skipper_state.json"
ENDPOINT="mock-pile1-1"
FRESH=0
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --fresh) FRESH=1; shift ;;
        -s|--state) STATE="$2"; shift 2 ;;
        -e|--endpoint) ENDPOINT="$2"; shift 2 ;;
        *) ARGS+=("$1"); shift ;;
    esac
done

if [ "$FRESH" = 1 ]; then rm -f "$STATE"; fi

export YDBD_MOCK_RUN_DIR="$RUN"
export PYTHONDONTWRITEBYTECODE=1

exec "$WORK/venv/bin/python" "$REPO/ydb/apps/bridge_skipper_demo/skipper.py" \
    -e "$ENDPOINT" -s "$STATE" --ydb "$HERE/ydb_shim.py" --log-level "${DEMO_LOG_LEVEL:-DEBUG}" \
    ${ARGS[@]+"${ARGS[@]}"}
