#!/usr/bin/env bash
echo
echo Starting S3 server
echo

CTX_DIR=$1
YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd" \
PYTHONPATH=$source_root/ydb/public/sdk/python3:$source_root/library/python/testing/yatest_common:$source_root/library/python/testing:$source_root \
python3 $source_root/ydb/tests/tools/s3_recipe/__main__.py start --output-dir $CTX_DIR --build-root "$build_root" --env-file $CTX_DIR/env.json
code=$?
if [ $code -gt 0 ];then
  echo
  echo "S3 server start failed"
  echo
  exit $code
fi

echo
echo S3 server started successfully
echo


