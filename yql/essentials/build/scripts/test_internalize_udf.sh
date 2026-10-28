#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "${BASH_SOURCE[0]%/*}" && pwd)"
toolchain="${YQL_UDF_TEST_TOOLCHAIN:-}"
if [[ -z "$toolchain" ]]; then
    compiler="$("$script_dir/../../../../ya" tool c++ --print-path)"
    toolchain="${compiler%/*}"
    toolchain="${toolchain%/*}"
fi

exec "${YQL_UDF_TEST_PYTHON:-python3}" "$script_dir/test_internalize_udf.py" --toolchain "$toolchain" --work-dir ./tmp "$@"
