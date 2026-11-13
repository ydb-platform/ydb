#!/usr/bin/env bash
# Start bridge_skipper, mocks and ydbd_mocks_control; Ctrl-C stops this lab.
# lab.sh [--fresh] [--piles pile1,pile2] [--nodes-per-pile 3]
#        [--skipper-port 8080] [--control-port 21999] [--topology FILE]
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../../.." && pwd)"
exec "$HERE/mock.sh" cluster lab \
    --binary "${BRIDGE_SKIPPER_BINARY:-$REPO/ydb/apps/bridge_skipper/bridge_skipper}" "$@"
