PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

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
    ydb/tests/stress/olap_workload/workload
)


END()
