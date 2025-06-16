PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/olap_workload/olap_workload")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/stress/olap_workload
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/common
)

END()
