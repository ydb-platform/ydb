#!/bin/bash
set -e
branch=$(git rev-parse --abbrev-ref HEAD)
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR/..

## Get google-benchmark
mkdir -p .bench
if [ ! -d .bench/google-benchmark ]; then
    git clone https://github.com/google/benchmark .bench/google-benchmark
fi
compare=$(realpath .bench/google-benchmark/tools/compare.py)

meson setup -Dbuild_benchmarks=true -Dbuild_ippbench=true --warnlevel 0 --buildtype release builddir-${branch}
cd builddir-${branch}
ninja
$compare filters ./benchexe $1 $2 --benchmark_repetitions=$3
