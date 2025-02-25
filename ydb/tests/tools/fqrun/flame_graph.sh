#!/usr/bin/env bash

set -eux

if [ $# -gt 2 ]; then
    echo "Too many arguments"
    exit -1
fi

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

$SCRIPT_DIR/../kqprun/flame_graph.sh ${1:-'30'} ${2:-''} fqrun
