#!/usr/bin/env bash

GRAFANA_API="http://admin:admin@localhost:3000/api"

curl -X POST -H "Content-Type: application/json" ${GRAFANA_API}/folders --data-ascii '{ "uid": "ydb", "title": "YDB" }'

for DASH in cpu dboverview dbstatus actors grpc queryengine txproxy datashard; do
    cat ../helm/ydb-prometheus/dashboards/${DASH}.json | jq '{ folderUid: "ydb", dashboard: . }' | curl -X POST -H "Content-Type: application/json" ${GRAFANA_API}/dashboards/db -d @-
done
