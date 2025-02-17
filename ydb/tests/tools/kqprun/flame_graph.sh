#!/usr/bin/env bash

set -eux

kqprun_pid=$(pgrep -u $USER kqprun)

sudo perf record -F 50 --call-graph dwarf -g --proc-map-timeout=10000 --pid $kqprun_pid -v -o profdata -- sleep ${1:-'30'}
sudo perf script -i profdata > profdata.txt

flame_graph_tool="../../../../contrib/tools/flame-graph/"

${flame_graph_tool}/stackcollapse-perf.pl profdata.txt | ${flame_graph_tool}/flamegraph.pl > profdata.svg
