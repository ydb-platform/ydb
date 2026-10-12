#!/bin/bash

set -e
set -x

pushd /home/vladilenmuz/ydbwork/ydb/ydb/core/tx/columnshard/tools/otel_logs_to_ydb/bin
trap 'popd' EXIT

pssh scp -p 10 otel_logs_to_ydb.yaml L@/home/vladilenmuz/arcadia/kikimr/ci/stability/resources/ydb_olap_perf/hosts_perf:/tmp/otel_logs_to_ydb.yaml

pssh run -p 10 "sudo install -m 755 /tmp/otel_logs_to_ydb.yaml /opt/otel_logs_to_ydb/otel_logs_to_ydb.yaml && sudo chown -R otel_logs_to_ydb:otel_logs_to_ydb /opt/otel_logs_to_ydb && sudo systemctl restart otel_logs_to_ydb" L@/home/vladilenmuz/arcadia/kikimr/ci/stability/resources/ydb_olap_perf/hosts_perf

date
