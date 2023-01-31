#!/bin/bash
set -e

optional_args=""

if [ -f "$YDBD_SERVICE_MAIN_DIR/token/ydbd.token" ]; then
  optional_args="--auth-token-file $YDBD_SERVICE_MAIN_DIR/token/ydbd.token"
fi

exec "$YDBD_SERVICE_MAIN_DIR/bin/ydbd" server \
  --yaml-config "$YDBD_SERVICE_MAIN_DIR/cfg/config.yaml" \
  --log-level 3 \
  --syslog \
  --tcp \
  --grpc-port 2135 \
  --ic-port 19001 \
  --mon-port 8765 \
  --node static $optional_args
