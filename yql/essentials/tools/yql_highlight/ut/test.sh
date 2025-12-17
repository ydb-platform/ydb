#!/bin/bash

set -e

cd "$(dirname "$0")"

MODE="${1:-yql}"
THEME="${2:-light}"

echo "Generating highlighting configurations..."
ya make ..

DIR=$(mktemp -d)
echo "Working directory is created at $DIR"

function copy() {
    cp "$1" "$DIR/$2"
}

cp "monarch.template.ts" "$DIR/monarch.template.ts"
cp "test.template.html" "$DIR/test.template.html"
cp "test.py" "$DIR/test.py"
cp "query.yql" "$DIR/query.yql"
cp "query.yqls" "$DIR/query.yqls"
cp "../artifact/YQL.monarch.json" "$DIR/YQL.monarch.json"
cp "../artifact/YQL.tmLanguage.json" "$DIR/YQL.tmLanguage.json"
cp "../artifact/YQL.highlightjs.json" "$DIR/YQL.highlightjs.json"
cp "../artifact/YQLs.monarch.json" "$DIR/YQLs.monarch.json"
cp "../artifact/YQLs.tmLanguage.json" "$DIR/YQLs.tmLanguage.json"
cp "../artifact/YQLs.highlightjs.json" "$DIR/YQLs.highlightjs.json"

cd "$DIR"

echo "Generating playground..."
python3 test.py

if [ "$MODE" == "monarch" ]; then
    echo "Openning Monarch Playground template..."
    python3 -m webbrowser -t "file://$DIR/monarch.patched.ts"
    python3 -m webbrowser -t "https://microsoft.github.io/monaco-editor/playground.html"
else
    echo "Openning in a web-browser..."
    python3 -m webbrowser -t "file://$DIR/test.patched.html?syntax=$MODE&theme=$THEME"
fi
