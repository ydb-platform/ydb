PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="TX_TIERING:DEBUG")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

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

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

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
    ydb/apps/ydb
)

END()

