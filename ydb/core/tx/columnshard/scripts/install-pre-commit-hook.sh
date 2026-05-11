#!/usr/bin/env bash
set -euo pipefail

# Installs the columnshard git pre-commit hook into .git/hooks/pre-commit (with backup).
# Run once from anywhere inside the repo:
#   bash ydb/core/tx/columnshard/scripts/install-pre-commit-hook.sh

ROOT="$(git rev-parse --show-toplevel)"
HOOK_SRC="$ROOT/ydb/core/tx/columnshard/git-hooks/pre-commit"
HOOK_DST="$ROOT/.git/hooks/pre-commit"

if [[ ! -f "$HOOK_SRC" ]]; then
    echo "Hook not found: $HOOK_SRC" >&2
    exit 1
fi

if [[ -f "$HOOK_DST" ]]; then
    bak="${HOOK_DST}.bak.$(date +%Y%m%d%H%M%S)"
    mv "$HOOK_DST" "$bak"
    echo "Existing pre-commit saved as $bak"
fi

cp "$HOOK_SRC" "$HOOK_DST"
chmod +x "$HOOK_DST"
echo "Installed: $HOOK_DST"
