PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")

SIZE(LARGE)
TAG(ya:fat)

TEST_SRCS(
    test_S3.py

)

PEERDIR(
    ydb/tests/stress/oltp_workload/workload
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

END()