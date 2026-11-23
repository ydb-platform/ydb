#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <executable>" >&2
    exit 2
fi

directory=$(dirname -- "$0")
exec python3 "$directory/stdio.py" \
    "$1" "$directory/../functional/traces/session/initialization.json"

