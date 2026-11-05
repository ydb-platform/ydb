#!/usr/bin/env bash
# Generate Python modules from the original YDB public protos, for runs outside ya.
#
#   gen_protos.sh [<repo-root> [<out-dir>]]
#
# Defaults: repo root is derived from this script's location, out-dir is
# $YDBD_MOCK_WORK/gen (YDBD_MOCK_WORK defaults to ~/ydbd_mock_work).
# The output keeps the ydb/public/api/... layout, so imports match the ya build.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${1:-$(cd "$HERE/../../../../.." && pwd)}"
WORK="${YDBD_MOCK_WORK:-$HOME/ydbd_mock_work}"
OUT="${2:-$WORK/gen}"
PY="${YDBD_MOCK_PYTHON:-$WORK/venv/bin/python}"

mkdir -p "$OUT"
cd "$REPO"

"$PY" -m grpc_tools.protoc -I "$REPO" \
    --python_out="$OUT" --grpc_python_out="$OUT" \
    ydb/public/api/protos/annotations/validation.proto \
    ydb/public/api/protos/ydb_common.proto \
    ydb/public/api/protos/ydb_issue_message.proto \
    ydb/public/api/protos/ydb_status_codes.proto \
    ydb/public/api/protos/ydb_operation.proto \
    ydb/public/api/protos/ydb_bridge_common.proto \
    ydb/public/api/protos/draft/ydb_bridge.proto \
    ydb/public/api/protos/ydb_discovery.proto \
    ydb/public/api/protos/ydb_config.proto \
    ydb/public/api/protos/ydb_monitoring.proto \
    ydb/public/api/grpc/ydb_discovery_v1.proto \
    ydb/public/api/grpc/ydb_config_v1.proto \
    ydb/public/api/grpc/ydb_monitoring_v1.proto \
    ydb/public/api/grpc/draft/ydb_bridge_v1.proto

echo "generated protos into $OUT"
