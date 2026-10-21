PY3TEST()

TEST_SRCS(
    common.py
    test_bridge.py
    test_conversion.py
    test_discovery.py
    test_pile_ops.py
    test_three_piles.py
    test_sdk_cleanup.py
    test_sdk_reconnect.py
)

SPLIT_FACTOR(24)

# Each case runs up to 24 YDB processes; do not co-schedule bridge clusters.
REQUIREMENTS(ram:48 cpu:all)
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
ELSE()
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
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
