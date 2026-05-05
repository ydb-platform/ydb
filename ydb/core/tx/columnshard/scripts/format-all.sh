#!/usr/bin/env bash
set -euo pipefail

# Reformats C++ under ydb/core/tx/columnshard with columnshard/.clang-format using the
# same clang-format binary as CI (ya host resource), not the system package.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

find_clang_format_path() {
    local c
    for c in clang-format-18 clang-format-17 clang-format-15 clang-format; do
        if command -v "$c" >/dev/null 2>&1; then
            command -v "$c"
            return 0
        fi
    done
    return 1
}

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
    echo "Run inside a git checkout of ydb" >&2
    exit 1
}

if CF="$("$SCRIPT_DIR/resolve-clang-format.sh" 2>/dev/null)"; then
    echo "Using CI/ya clang-format: $CF"
else
    CF="$(find_clang_format_path)" || {
        echo "clang-format not found. Run once from repo root: ./ya tool clang-format-18 --version" >&2
        exit 1
    }
    echo "WARNING: using PATH clang-format ($CF), not ya bundle — may differ from CI; prefer: ./ya tool clang-format-18 --version" >&2
fi

PREFIX="ydb/core/tx/columnshard"
cd "$ROOT"

STYLE_FILE="${PREFIX}/.clang-format"
echo "style=file:${STYLE_FILE}"

find "$PREFIX" -type f \( -name '*.cpp' -o -name '*.h' -o -name '*.hpp' \) -print0 |
    xargs -0 -P "$(nproc 2>/dev/null || echo 8)" "$CF" -i "--style=file:${STYLE_FILE}"

echo "Done."
