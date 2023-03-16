#!/bin/bash

python ${source_root}/ydb/tests/oss/prepare/compile_protos.py --source-root ${source_root}

cd ${source_root}/ydb/tests/functional
mkdir test-results
mkdir test-results/py3test
mkdir test-results/py3test/testing_out_stuff

cd test-results
python ${source_root}/ydb/tests/oss/prepare/generate_test_context.py --build-root ${build_root} --source-root ${source_root}
cd ..

export PYTHONPATH=${source_root}:${source_root}/library/python/testing/yatest_common:${source_root}/library/python/testing:${source_root}/library/python/pytest/plugins:${source_root}/ydb/tests/oss/canonical

export YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd"
export PYTEST_PLUGINS=ya,conftests,canonical
export YA_TEST_CONTEXT_FILE=${source_root}/ydb/tests/functional/test-results/test.context
export YDB_OPENSOURCE=yes
