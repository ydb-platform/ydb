PY3TEST()

TEST_SRCS(
    test_bridge_chaos.py
)

# The cluster grows to 16 YDB processes with file drives, and the random phase alone lasts BRIDGE_CHAOS_SECONDS.
REQUIREMENTS(ram:48 cpu:all)
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
TIMEOUT(3600)

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

END()
