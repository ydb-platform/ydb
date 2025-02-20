PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="TX_TIERING:DEBUG")

TEST_SRCS(
    base.py
    data_correctness.py
    ttl_delete_s3.py
    ttl_unavailable_s3.py
    data_migration_when_alter_ttl.py
    unstable_connection.py
)

SIZE(MEDIUM)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    contrib/python/boto3
    library/recipes/common
)

DEPENDS(
    ydb/apps/ydbd
    contrib/python/moto/bin
)

END()

