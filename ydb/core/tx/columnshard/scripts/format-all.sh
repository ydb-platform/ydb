#!/usr/bin/env bash
set -euo pipefail

# Format columnshard using its Ya autoinclude configuration.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)" || {
    echo "Run inside a git checkout of ydb" >&2
    exit 1
}

if [[ $# -eq 0 ]]; then
    set -- --fix
fi
exec python3 "$ROOT/.github/scripts/cpp_format/format.py" --root ydb/core/tx/columnshard "$@"
