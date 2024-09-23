#!/usr/bin/env bash

# For svg graph download https://github.com/brendangregg/FlameGraph
# and run `FlameGraph/stackcollapse-perf.pl profdata.txt | FlameGraph/flamegraph.pl > profdata.svg`

pid=$(pgrep -u $USER kqprun)

echo "Target process id: ${pid}"

sudo perf record -F 50 --call-graph dwarf -g --proc-map-timeout=10000 --pid $pid -v -o profdata -- sleep 30
sudo perf script -i profdata > profdata.txt
