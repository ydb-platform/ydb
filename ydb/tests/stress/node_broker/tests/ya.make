PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_TEST_PATH="ydb/tests/stress/node_broker/node_broker")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/stress/node_broker
)

PEERDIR(
    ydb/tests/library
    ydb/tests/stress/node_broker/workload
)

END()
