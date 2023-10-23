#!/usr/bin/env bash
set -o pipefail
echo Compiling protospecs for Python...
# Usage: compile_protos.sh root_ydb_repo_dir subdir_to_scan_for_protospecs
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
python3 $SCRIPT_DIR/compile_protos.py --source-root $1 --strict-mode 1 "${@:2}" 2>&1 >/dev/null | grep -v "No syntax specified for the proto file"
