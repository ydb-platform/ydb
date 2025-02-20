#!/usr/bin/env bash

set -eux

function cleanup {
    sudo rm ./profdata
    rm ./profdata.txt
}
trap cleanup EXIT

if [ $# -gt 1 ]; then
    echo "Too many arguments"
    exit -1
fi

fqrun_pid=$(pgrep -u $USER fqrun)

sudo perf record -F 50 --call-graph dwarf -g --proc-map-timeout=10000 --pid $fqrun_pid -v -o profdata -- sleep ${1:-'30'}
sudo perf script -i profdata > profdata.txt

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

flame_graph_tool="$SCRIPT_DIR/../../../../contrib/tools/flame-graph/"

${flame_graph_tool}/stackcollapse-perf.pl profdata.txt | ${flame_graph_tool}/flamegraph.pl > profdata.svg
