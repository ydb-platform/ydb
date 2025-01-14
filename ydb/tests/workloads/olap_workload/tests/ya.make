PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_workload.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:20)

DEPENDS(
    ydb/apps/ydbd
    ydb/apps/ydb
    ydb/tests/workloads/olap_workload
)

PEERDIR(
    ydb/tests/library
)


END()
