#!/bin/bash
set -e

ydbd_service_config="$YDBD_SERVICE_MAIN_DIR/config.json"

read_config_value_or_default() {
  field=$1
  default=$2
  value=$(
    jq \
      --arg field $field \
      -r \
      '.[$field]' "$ydbd_service_config"
  )

  if [ "$value" == "null" ]; then
    echo "$default"
  else
    echo "$value"
  fi
}

taskset=""
optional_args=""

if [ -f "$YDBD_SERVICE_MAIN_DIR/token/ydbd.token" ]; then
  optional_args="--auth-token-file $YDBD_SERVICE_MAIN_DIR/token/ydbd.token"
fi

if [ -f "$ydbd_service_config" ]; then
  ydbd_service_minidumps_path=$(read_config_value_or_default minidumps_path "")
  if [ -n "$ydbd_service_minidumps_path" ]; then
      export LD_PRELOAD=libbreakpad_init.so
      export BREAKPAD_MINIDUMPS_PATH="$ydbd_service_minidumps_path"
  fi

  ydbd_service_taskset=$(read_config_value_or_default taskset "")
  if [ -n "$ydbd_service_taskset" ]; then
      taskset="taskset -c $ydbd_service_taskset"
  fi
fi


exec $taskset "$YDBD_SERVICE_MAIN_DIR/bin/ydbd" server \
  --yaml-config "$YDBD_SERVICE_MAIN_DIR/cfg/config.yaml" \
  --log-level 3 \
  --syslog \
  --tcp \
  --grpc-port 2135 \
  --ic-port 19001 \
  --mon-port 8765 \
  --node static $optional_args
