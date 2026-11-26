PY3TEST()

STYLE_PYTHON()

ENV(
    YDB_USE_IN_MEMORY_PDISKS=false
    YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd"
)

PEERDIR(
    contrib/python/boto3
    contrib/python/pytest
    contrib/python/requests
    library/python/port_manager
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/tests/fq/streaming_common
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/python
    ydb/tests/library
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    contrib/python/moto/bin
)

TEST_SRCS(
    test_insert_restarts.py
)

PY_SRCS(
    conftest.py
)

REQUIREMENTS(cpu:1)
IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
