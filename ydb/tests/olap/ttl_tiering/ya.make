PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="TX_TIERING:DEBUG")

FORK_TESTS()

TEST_SRCS(
    base.py
    data_correctness.py
    data_migration_when_alter_ttl.py
    tier_delete.py
    ttl_delete_s3.py
    ttl_unavailable_s3.py
    unstable_connection.py
)

SIZE(MEDIUM)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/tests/olap/lib
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    contrib/python/boto3
    library/recipes/common
    ydb/tests/olap/common
)

DEPENDS(
    contrib/python/moto/bin
)

END()

