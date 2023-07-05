#!/usr/bin/env bash

set -e
set -x

/ydb -e grpcs://localhost:${GRPC_TLS_PORT:-2135} --ca-file /ydb_certs/ca.pem -d /local scheme ls /local
/ydb -e grpcs://localhost:${GRPC_TLS_PORT:-2135} --ca-file /ydb_certs/ca.pem -d /local table query execute -q 'create table `/local/.sys_health/test` (key int32, value utf8, primary key(key));' -t scheme
