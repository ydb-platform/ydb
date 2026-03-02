PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

TEST_SRCS(
    test_streaming.py
    test_watermarks.py
)

IF (OS_LINUX)
    TEST_SRCS(
        test_udfs.py
    )
ENDIF()

PY_SRCS(
    common.py
    conftest.py
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:20)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(ram:12)
ENDIF()

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    library/recipes/common
    ydb/tests/olap/common
    ydb/tests/tools/datastreams_helpers
)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/tools/pq_read
    yql/essentials/udfs/common/python/python3_small
)

END()
