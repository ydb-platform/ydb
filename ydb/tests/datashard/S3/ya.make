PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="TX_TIERING:DEBUG")


SIZE(LARGE)
TAG(ya:fat)

TEST_SRCS(
    test_S3.py

)

PEERDIR(
    ydb/tests/stress/oltp_workload/workload
    ydb/tests/sql/lib
    contrib/python/moto
    contrib/python/boto3
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    contrib/python/moto/bin
)

END()