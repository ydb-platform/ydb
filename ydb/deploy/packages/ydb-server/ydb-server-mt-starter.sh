#!/bin/bash
set -e

ydbd_service_tenants_config="$YDBD_SERVICE_TENANTS_DIR/config.json"
tenants=$(jq -r '. | keys | join("\n")' "$ydbd_service_tenants_config")

for tenant in $tenants; do
  systemctl start "ydb-server-mt-tenant@$tenant"
done
