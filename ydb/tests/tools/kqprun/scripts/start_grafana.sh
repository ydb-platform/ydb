#!/usr/bin/env bash

set -eux

if [ $# -le 0 ]; then
    echo "Please provide prometheus port"
    exit -1
fi

if [ $# -le 1 ]; then
    echo "Please provide grafana web ui port"
    exit -1
fi

SCRIPT_DIR=$(cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)
DASHBOARDS_DIRS="$SCRIPT_DIR/../../../../deploy/helm/ydb-prometheus/dashboards/"
if [ $# -gt 2 ]; then
    DASHBOARDS_DIRS="$DASHBOARDS_DIRS $3"
fi

if [ $# -gt 3 ]; then
    echo "Too many arguments"
    exit -1
fi

CONTAINER_NAME=$USER-kqprun-grafana-$1-$2

$SCRIPT_DIR/cleanup_docker.sh $CONTAINER_NAME
docker run -d --rm --name=$CONTAINER_NAME --network host -e "GF_SERVER_HTTP_PORT=$2" grafana/grafana-oss
docker container ls --filter name=$CONTAINER_NAME

echo "Wait grafana is up"

(set -eux; while ! nc -v -z -w 1 localhost $2; do sleep 2; done)

echo "Initialization of datasource and dashboards"

GRAFANA_API="http://admin:admin@localhost:$2/api"

curl -X POST -H "Content-Type: application/json" ${GRAFANA_API}/datasources --data-ascii "{ \"name\": \"prometheus_datasource_$1\", \"type\": \"prometheus\", \"url\": \"http://localhost:$1\", \"access\": \"proxy\" }"
curl -X POST -H "Content-Type: application/json" ${GRAFANA_API}/folders --data-ascii '{ "uid": "ydb", "title": "YDB" }'

for DASHBOARDS_DIR in $DASHBOARDS_DIRS; do
    for DASH in "$DASHBOARDS_DIR"/*; do
        jq '{ folderUid: "ydb", dashboard: . }' < "$DASH" | curl -X POST -H "Content-Type: application/json" ${GRAFANA_API}/dashboards/db -d @-
    done
done

echo "Initialization finished successfully"
