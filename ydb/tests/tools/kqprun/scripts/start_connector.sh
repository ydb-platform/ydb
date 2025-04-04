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

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
CONTAINER_NAME=$USER-kqprun-fq-connector-go-$1

$SCRIPT_DIR/cleanup_docker.sh $CONTAINER_NAME
docker pull ghcr.io/ydb-platform/fq-connector-go:latest
docker run -d --rm --name=$CONTAINER_NAME --network host ghcr.io/ydb-platform/fq-connector-go:latest --connector-port=$1 --metrics-port=$(($1 + 1)) --pprof-port=$(($1 + 2))
docker container ls --filter name=$CONTAINER_NAME
