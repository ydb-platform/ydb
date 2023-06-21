#!/bin/bash

python ${source_root}/ydb/tests/oss/launch/compile_protos.py --source-root ${source_root} 2>/dev/null

testresults=${source_root}/ydb/tests/functional/test-results

mkdir ${testresults}
mkdir ${testresults}/xml
mkdir ${testresults}/py3test
mkdir ${testresults}/py3test/testing_out_stuff

python ${source_root}/ydb/tests/oss/launch/generate_test_context.py --build-root ${build_root} --source-root ${source_root} --out-dir ${testresults}

export PYTHONPATH=${source_root}/ydb/public/sdk/python3:${source_root}:${source_root}/library/python/testing/yatest_common:${source_root}/library/python/testing:${source_root}/library/python/pytest/plugins:${source_root}/ydb/tests/oss/canonical

export YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd"
export YDB_CLI_BINARY="ydb/apps/ydb/ydb"
export SQS_CLIENT_BINARY="ydb/core/ymq/client/bin/sqs"
export PYTEST_PLUGINS=ya,conftests,canonical
export YA_TEST_CONTEXT_FILE=${testresults}/test.context
export YDB_OPENSOURCE=yes
