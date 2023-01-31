#!/bin/bash
set -e

tenants=$(
  find "$YDBD_SERVICE_TENANTS_DIR" \
    -maxdepth 1 \
    -mindepth 1 \
    -type d \
    -exec basename {} \;
)

for tenant in $tenants; do
  systemctl start "ydb-server-mt-tenant@$tenant"
done
