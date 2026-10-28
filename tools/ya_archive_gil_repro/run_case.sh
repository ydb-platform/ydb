#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 4 ]]; then
    echo 'Usage: run_case.sh YA_BIN CASE_DIR ARCHIVE original|release' >&2
    exit 2
fi
probe_bin=$1
probe_case=$2
probe_archive=$3
probe_mode=$4
probe_source=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
python3 "$probe_source/prepare.py" --run-dir "$probe_case" --archive "$probe_archive"
export Y_PYTHON_ENTRY_POINT=:main
export PYTHONDONTWRITEBYTECODE=1
export GIL_PROBE_DIR="$probe_case"
export GIL_PROBE_MODE="$probe_mode"
# Caller's YA_CACHE_DIR isolates build and tools cache. Graph nodes have cache=false.
"$probe_bin" "$probe_source/ya_probe.py" make --build relwithdebinfo \
    --build-custom-json "$probe_case/graph.json" \
    --no-src-links --no-bazel-remote-store --link-threads 2 \
    --output "$probe_case/out" --log-file "$probe_case/ya.log" \
    --evlog-file "$probe_case/evlog.jsonl" 2>&1 | tail -n 30
