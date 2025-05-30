PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/tests/stress/common
    ydb/tests/stress/node_broker/workload
)

END()
