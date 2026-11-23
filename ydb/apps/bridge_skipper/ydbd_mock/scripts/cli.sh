#!/usr/bin/env bash
# Guarded ydb CLI: only talks to ydbd_mock endpoints (see ydb_shim.py).
#
#   cli.sh -e grpc://mock-pile1-1:2135 admin cluster bridge list --format=json
#   cli.sh -e grpc://127.0.0.1:22100 monitoring healthcheck ...
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export YDBD_MOCK_RUN_DIR="${YDBD_MOCK_RUN_DIR:-${YDBD_MOCK_WORK:-$HOME/ydbd_mock_work}/run}"
exec "$HERE/ydb_shim.py" "$@"
