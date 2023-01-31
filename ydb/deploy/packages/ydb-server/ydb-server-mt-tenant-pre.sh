#!/bin/bash
set -e

# vars passed as environments

ydbd_service_pid_path="/run/$YDBD_SERVICE_USER"

install -o "$YDBD_SERVICE_USER" -d "$ydbd_service_pid_path"
install -o root -d "$YDBD_SERVICE_TENANT_DIR"
install -o syslog -d "$YDBD_SERVICE_TENANT_DIR/logs"
install -o "$YDBD_SERVICE_USER" -d "$YDBD_SERVICE_TENANT_DIR/cache"
