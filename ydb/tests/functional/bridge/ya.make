PY3TEST()

TEST_SRCS(
    test_bridge.py
)

SPLIT_FACTOR(10)

REQUIREMENTS(ram:32 cpu:32)
SIZE(LARGE)
TAG(ya:fat)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
ELSE()
    TIMEOUT(60)
ENDIF()


ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(IAM_TOKEN="")
DEPENDS(
    ydb/apps/ydbd
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
