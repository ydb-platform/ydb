#!/usr/bin/env bash
./arun "$@" &
sudo perf record --pid $! -v -g -o ~/profdata-all
sudo perf script -i ~/profdata-all > ~/profdata-all.txt
~/arcadia/contrib/tools/flame-graph/stackcollapse-perf.pl ~/profdata-all.txt > ~/profdata-all.txt.collapsed
~/arcadia/contrib/tools/flame-graph/flamegraph.pl ~/profdata-all.txt.collapsed > ~/perf.svg
