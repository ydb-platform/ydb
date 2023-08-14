#!/usr/bin/env bash
echo
echo Starting YDB server
echo

CTX_DIR=$1
YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd" \
PYTHONPATH=$source_root/ydb/public/sdk/python3:$source_root/library/python/testing/yatest_common:$source_root/library/python/testing:$source_root \
python3 $source_root/ydb/public/tools/ydb_recipe/__main__.py start --output-dir $CTX_DIR --build-root "$build_root" --ydb-working-dir $CTX_DIR
code=$?
if [ $code -gt 0 ];then
  echo
  echo "YDB server start failed"
  echo
  exit $code
fi

echo
echo YDB server started successfully
echo


