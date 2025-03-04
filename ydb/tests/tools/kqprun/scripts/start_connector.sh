#!/usr/bin/env bash

set -eux

if [ $# -le 0 ]; then
    echo "Please provide connector port (value of Generic::Connector::Endpoint::Port in config)"
    exit -1
fi

if [ $# -gt 2 ]; then
    echo "Too many arguments"
    exit -1
fi

docker pull ghcr.io/ydb-platform/fq-connector-go:latest
docker run --rm --name=fq-connector-go-$1 --network host ghcr.io/ydb-platform/fq-connector-go:latest --connector-port=$1 --metrics-port=$(($1 + 1)) --pprof-port=$(($1 + 2))
