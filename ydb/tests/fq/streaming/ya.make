PY3TEST()

FORK_SUBTESTS()

SPLIT_FACTOR(50)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

TEST_SRCS(
    test_early_finish.py
    test_streaming.py
    test_watermarks.py
)

IF (OS_LINUX)
    TEST_SRCS(
        test_udfs.py
    )
ENDIF()

PY_SRCS(
    conftest.py
)


REQUIREMENTS(cpu:4)
REQUIREMENTS(ram:20)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

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
