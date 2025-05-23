PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_KQP_ENABLE_BATCH_UPDATES="true")

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
    ydb/tests/stress/batch_operations/workload
)

END()
