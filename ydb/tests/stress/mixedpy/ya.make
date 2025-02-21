PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")


TEST_SRCS(
    test_mixed.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ELSE()
    REQUIREMENTS(ram:16)
ENDIF()

TIMEOUT(1200)
SIZE(LARGE)
TAG(ya:fat)

DEPENDS(
    ydb/apps/ydbd
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
)


END()
