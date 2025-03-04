#!/usr/bin/env bash

set -eux

if [ $# -le 0 ]; then
    echo "Please provide monitoring port to listen (value of -M argument)"
    exit -1
fi

if [ $# -le 1 ]; then
    echo "Please provide prometheus web ui port"
    exit -1
fi

if [ $# -gt 3 ]; then
    echo "Too many arguments"
    exit -1
fi

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)

function cleanup {
    rm $SCRIPT_DIR/prometheus_config.yaml
}
trap cleanup EXIT

sed "s/\${TARGET_PORT}/$1/g" "${3:-$SCRIPT_DIR/../configuration/prometheus_config.yaml}" > $SCRIPT_DIR/prometheus_config.yaml

docker run --rm --name=prometheus-$1-$2 --network host -v $SCRIPT_DIR/prometheus_config.yaml:/etc/prometheus/prometheus.yml prom/prometheus --config.file=/etc/prometheus/prometheus.yml --web.listen-address=:$2
