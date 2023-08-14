#!/usr/bin/env bash
echo
echo Stopping YDB server
echo

CTX_DIR=$1
YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd" \
PYTHONPATH=$source_root/ydb/public/sdk/python3:$source_root/library/python/testing/yatest_common:$source_root/library/python/testing:$source_root \
python3 $source_root/ydb/public/tools/ydb_recipe/__main__.py stop --output-dir $CTX_DIR --build-root "$build_root" --ydb-working-dir $CTX_DIR

