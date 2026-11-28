PY3TEST()

STYLE_PYTHON()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

PEERDIR(
    contrib/python/boto3
    library/python/port_manager
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/tests/fq/streaming_common
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    contrib/python/pytest
    contrib/python/requests
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    contrib/python/moto/bin
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TEST_SRCS(
    test_stats_mode.py
)

PY_SRCS(
    conftest.py
)

END()
