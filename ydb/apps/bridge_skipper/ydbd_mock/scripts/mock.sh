#!/usr/bin/env bash
# Run the mock: one instance (`run ...`) or the cluster supervisor (`cluster ...`).
#
#   mock.sh run --topology FILE --pile pile1
#   mock.sh cluster up --piles pile1,pile2
#   mock.sh cluster status | state | events | kill pile2 | ...
#
# YDBD_MOCK_IMPL=venv (default) runs `python -m ydb.apps.bridge_skipper.ydbd_mock`
# from the virtualenv; YDBD_MOCK_IMPL=ya runs the binary built by
#   ./ya make --build relwithdebinfo ydb/apps/bridge_skipper/ydbd_mock
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../../.." && pwd)"
WORK="${YDBD_MOCK_WORK:-$HOME/ydbd_mock_work}"

export YDBD_MOCK_WORK="$WORK"
export YDBD_MOCK_RUN_DIR="${YDBD_MOCK_RUN_DIR:-$WORK/run}"
export PYTHONDONTWRITEBYTECODE=1

case "${YDBD_MOCK_IMPL:-venv}" in
    venv)
        export PYTHONPATH="$REPO:$WORK/gen${PYTHONPATH:+:$PYTHONPATH}"
        exec "$WORK/venv/bin/python" -m ydb.apps.bridge_skipper.ydbd_mock "$@"
        ;;
    ya)
        BIN="$REPO/ydb/apps/bridge_skipper/ydbd_mock/ydbd_mock"
        export YDBD_MOCK_EXE="$BIN"
        exec "$BIN" "$@"
        ;;
    *)
        echo "mock.sh: unknown YDBD_MOCK_IMPL='${YDBD_MOCK_IMPL}' (venv|ya)" >&2
        exit 2
        ;;
esac
