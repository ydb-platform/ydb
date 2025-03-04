#!/usr/bin/env bash

set -eux

if [ $# -gt 1 ]; then
    echo "Too many arguments"
    exit -1
fi

KQPRUN_CONTAINERS=$(docker container ls -q --filter name=${1:-$USER-kqprun-*})

if [ "$KQPRUN_CONTAINERS" ]; then
    docker container stop $KQPRUN_CONTAINERS
else
    echo "Nothing to cleanup"
fi
