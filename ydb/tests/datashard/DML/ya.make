PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TIMEOUT(1800)
SIZE(LARGE)
TAG(ya:fat)
TEST_SRCS(
    test_DML.py
)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/stress/oltp_workload/workload
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

END()