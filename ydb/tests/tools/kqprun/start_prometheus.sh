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

echo "Checking monitoring port $1"
nc -v -z -w 5 localhost $1

if netstat -tuln | grep ":$2"; then
    echo "Can not start prometheus on port $2, port already in use"
    exit -1
fi

cp ${3:-'./configuration/prometheus_config.yaml'} ./prometheus_config.yaml
sed -i "s/\${TARGET_PORT}/$1/g" ./prometheus_config.yaml

docker run --rm --name=prometeus-$1 --network host -v ./prometheus_config.yaml:/etc/prometheus/prometheus.yml prom/prometheus --config.file=/etc/prometheus/prometheus.yml --web.listen-address=:$2
