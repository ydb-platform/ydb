#!/bin/sh
set -e

opt="/opt/ydb"
optional_args=""

if [ -f "$opt/token/ydbd.token" ]; then
  optional_args="--auth-token-file $opt/token/ydbd.token"
fi

exec $opt/bin/ydbd server \
  --log-level 3 \
  --syslog \
  --tcp \
  --yaml-config $opt/cfg/config.yaml \
  --grpc-port 2135 \
  --ic-port 19001 \
  --mon-port 8765 \
  --node static $optional_args
