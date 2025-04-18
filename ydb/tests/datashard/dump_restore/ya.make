PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

SIZE(LARGE)
TAG(ya:fat)

TEST_SRCS(
    test_dump_restore.py

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
