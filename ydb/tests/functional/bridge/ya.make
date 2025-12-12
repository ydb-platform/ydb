PY3TEST()

TEST_SRCS(
    common.py
    test_bridge.py
    test_discovery.py
)

SPLIT_FACTOR(10)

REQUIREMENTS(ram:32 cpu:32)


IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(600)
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
    ydb/tests/library/kv
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
