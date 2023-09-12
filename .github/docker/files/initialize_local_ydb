#!/bin/bash

INIT_YDB_SCRIPT=/init_ydb

export YDB_LOCAL_SURVIVE_RESTART="true"
export YDB_GRPC_ENABLE_TLS="true"
export GRPC_TLS_PORT=${GRPC_TLS_PORT:-2135}
export GRPC_PORT=${GRPC_PORT:-2136}
export YDB_GRPC_TLS_DATA_PATH="/ydb_certs"

/local_ydb deploy --ydb-working-dir /ydb_data --ydb-binary-path /ydbd --fixed-ports --dont-use-log-files;

if [ -f "$INIT_YDB_SCRIPT" ]; then
  sh "$INIT_YDB_SCRIPT";
fi

tail -f /dev/null
