PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

FORK_SUBTESTS()

SIZE(MEDIUM)

TEST_SRCS(
    test_TTL.py

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
