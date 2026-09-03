#!/usr/bin/env bash

set -euo pipefail

if (( $# != 0 )); then
    echo "Usage: $0" >&2
    exit 2
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

ya make "$ROOT"
mkdir -p "$ROOT/playground/src/generated"

TOOL="$ROOT/yql_highlight"

function tool() {
    language="$1"
    generate="$2"
    mode="$3"
    output="$4"

    "$TOOL" \
        --language="$language" \
        --generate="$generate" \
        --mode="$mode" \
        --output="$ROOT/playground/src/generated/$output"
}

tool yql  monarch     default  YQL.monarch.json
tool yql  monarch     ansi     YQL.ansi.monarch.json
tool yql  tmlanguage  default  YQL.tmLanguage.json
tool yql  tmlanguage  ansi     YQL.ansi.tmLanguage.json
tool yql  highlightjs default  YQL.highlightjs.json
tool yqls monarch     default  YQLs.monarch.json
tool yqls tmlanguage  default  YQLs.tmLanguage.json
tool yqls highlightjs default  YQLs.highlightjs.json
