#!/usr/bin/env bash
# Launch the C++ bridge_skipper using the running mock's direct node endpoints.
# skipper.sh [--listen HOST:PORT] [--state-dir DIR]
# BRIDGE_SKIPPER_BINARY overrides the built executable.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../../.." && pwd)"
exec "$HERE/mock.sh" cluster skipper \
    --binary "${BRIDGE_SKIPPER_BINARY:-$REPO/ydb/apps/bridge_skipper/bridge_skipper}" "$@"
