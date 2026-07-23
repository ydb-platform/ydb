#!/usr/bin/env bash
# Backward-compatible wrapper for increment graph planning.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PLAN_MODE=increment_graph
exec "$SCRIPT_DIR/plan_graph_shards.sh" "$@"
