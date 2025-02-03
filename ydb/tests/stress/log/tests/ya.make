PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    REQUIREMENTS(ram:32)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(ram:16)
ENDIF()


DEPENDS(
    ydb/apps/ydbd
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
)


END()
