PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)


TEST_SRCS(
    test_cluster_restarts.py
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
TAG(ya:fat)


PY_SRCS(
    conftest.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    library/recipes/common
    ydb/tests/olap/common
    ydb/tests/tools/datastreams_helpers
    ydb/tests/fq/streaming_common
)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/tools/pq_read
    yql/essentials/udfs/common/python/python3_small
)

END()
