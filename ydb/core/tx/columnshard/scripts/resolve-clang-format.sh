#!/usr/bin/env bash
# Prints absolute path to the clang-format binary used by cpp_style / CI
# (host resource from build/platform/clang/clang-format/clang-format18.json).
# Exits 1 if not found; run from repo root: ./ya tool clang-format-18 --version
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 1
JSON="${ROOT}/build/platform/clang/clang-format/clang-format18.json"
[[ -f "$JSON" ]] || exit 1

SBR="$(
    export JSON
    python3 <<'PY'
import json
import os
import platform

j = json.load(open(os.environ["JSON"]))
sysname = platform.system().lower()
machine = platform.machine().lower()
if sysname == "linux":
    key = "linux-aarch64" if machine in ("aarch64", "arm64") else "linux-x86_64"
elif sysname == "darwin":
    key = "darwin-arm64" if machine == "arm64" else "darwin-x86_64"
else:
    key = "linux-x86_64"
uri = j["by_platform"][key]["uri"]
print(uri.split(":")[-1])
PY
)"

TOOL="${HOME}/.ya/tools/v4/${SBR}/clang-format"
if [[ -x "$TOOL" ]]; then
    echo "$TOOL"
    exit 0
fi

if [[ -x "${ROOT}/ya" ]]; then
    (cd "$ROOT" && ./ya tool clang-format-18 --version >/dev/null 2>&1) || true
fi

if [[ -x "$TOOL" ]]; then
    echo "$TOOL"
    exit 0
fi

exit 1
