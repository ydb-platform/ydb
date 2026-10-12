#!/bin/bash

set -e
set -x

pssh run -p 10 "sudo systemctl stop otel_logs_to_ydb" L@/home/vladilenmuz/arcadia/kikimr/ci/stability/resources/ydb_olap_perf/hosts_perf

sleep 120

pssh run -p 10 "sudo systemctl start otel_logs_to_ydb" L@/home/vladilenmuz/arcadia/kikimr/ci/stability/resources/ydb_olap_perf/hosts_perf

date
