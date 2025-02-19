#!/usr/bin/env bash

set -eux

if [ $# -le 0 ]; then
    echo "Please provide connector port (value of Generic::Connector::Endpoint::Port in config)"
    exit -1
fi

if [ $# -gt 1 ]; then
    echo "Too many arguments"
    exit -1
fi

if netstat -tuln | grep ":$1"; then
    echo "Can not start connector on port $1, port already in use"
    exit -1
fi

cp ${3:-'./configuration/connector_config.yaml'} ./connector_config.yaml
sed -i "s/\${TARGET_PORT}/$1/g" ./connector_config.yaml

docker run --rm --name=fq-connector-go-$1 --network host -v ./connector_config.yaml:/opt/ydb/cfg/fq-connector-go.yaml ghcr.io/ydb-platform/fq-connector-go:latest
