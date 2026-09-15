#!/usr/bin/env bash

set -eux

if [ $# -gt 3 ]; then
    echo "Too many arguments"
    exit -1
fi

COLLECTION_TIME=${1:-'30'}
echo "Flame graph collection will be finished in $COLLECTION_TIME seconds or by ^C"

SUDO=""

if (( ${2:-''} )); then
    SUDO="sudo"
fi

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
ROOT_DIR=$(cd $SCRIPT_DIR && git rev-parse --show-toplevel 2>/dev/null || echo "$SCRIPT_DIR/../../../../../..")
FLAME_GRAPH_TOOL="$ROOT_DIR/contrib/tools/flame-graph/"

TARGET_PID=$(pgrep -u $USER ${3:-'kqprun'})

function finish {
    $SUDO perf script -i $SCRIPT_DIR/profdata > $SCRIPT_DIR/profdata.txt

    ${FLAME_GRAPH_TOOL}/stackcollapse-perf.pl $SCRIPT_DIR/profdata.txt | ${FLAME_GRAPH_TOOL}/flamegraph.pl > ./profdata.svg

    $SUDO rm $SCRIPT_DIR/profdata
    rm $SCRIPT_DIR/profdata.txt
}
trap finish EXIT

$SUDO perf record -F 50 --call-graph dwarf -g --proc-map-timeout=10000 --pid $TARGET_PID -v -o $SCRIPT_DIR/profdata -- sleep $COLLECTION_TIME
