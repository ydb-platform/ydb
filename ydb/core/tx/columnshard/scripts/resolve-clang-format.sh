#!/usr/bin/env bash
# Compatibility entry point for the shared Ya formatter resolver.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)" || exit 1
exec python3 "$ROOT/.github/scripts/cpp_format/format.py" --print-binary
