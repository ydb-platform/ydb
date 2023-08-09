#!/bin/bash

${source_root}/ydb/tests/oss/launch/compile_protos.sh ${source_root} ydb library/cpp/actors

#testresults=${source_root}/ydb/tests/functional/test-results
#
#mkdir ${testresults}
#mkdir ${testresults}/xml
#mkdir ${testresults}/py3test
#mkdir ${testresults}/py3test/testing_out_stuff
#
#python ${source_root}/ydb/tests/oss/launch/generate_test_context.py --build-root ${build_root} --source-root ${source_root} --out-dir ${testresults}

export PYTHONPATH=${source_root}/ydb/public/sdk/python3
export PYTHONPATH=$PYTHONPATH:${source_root}
export PYTHONPATH=$PYTHONPATH:${source_root}/library/python/testing/yatest_common
export PYTHONPATH=$PYTHONPATH:${source_root}/library/python/testing
export PYTHONPATH=$PYTHONPATH:${source_root}/library/python/pytest/plugins
export PYTHONPATH=$PYTHONPATH:${source_root}/ydb/tests/oss/canonical
export PYTHONPATH=$PYTHONPATH:${source_root}/ydb/tests/oss/ci


export YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd"
export YDB_CLI_BINARY="ydb/apps/ydb/ydb"
export SQS_CLIENT_BINARY="ydb/core/ymq/client/bin/sqs"
export PYTEST_PLUGINS=ya,ci,canonical
#export YA_TEST_CONTEXT_FILE=${testresults}/test.context
export YDB_OPENSOURCE=yes
