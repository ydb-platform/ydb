#!/usr/bin/env bash

set -eux

if [ $# -gt 3 ]; then
    echo "Too many arguments"
    exit -1
fi

SUDO=""

if [ ${2:-''} ]; then
    SUDO="sudo"
fi

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

function cleanup {
    $SUDO rm $SCRIPT_DIR/profdata
    rm $SCRIPT_DIR/profdata.txt
}
trap cleanup EXIT

TARGER_PID=$(pgrep -u $USER ${3:-'kqprun'})

$SUDO perf record -F 50 --call-graph dwarf -g --proc-map-timeout=10000 --pid $TARGER_PID -v -o $SCRIPT_DIR/profdata -- sleep ${1:-'30'}
$SUDO perf script -i $SCRIPT_DIR/profdata > $SCRIPT_DIR/profdata.txt

flame_graph_tool="$SCRIPT_DIR/../../../../../contrib/tools/flame-graph/"

${flame_graph_tool}/stackcollapse-perf.pl $SCRIPT_DIR/profdata.txt | ${flame_graph_tool}/flamegraph.pl > ./profdata.svg
