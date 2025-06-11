PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="ydb/tests/stress/cdc/cdc")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32)
SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/stress/cdc
)

PEERDIR(
    ydb/tests/library
    ydb/tests/stress/cdc/workload
)

END()
