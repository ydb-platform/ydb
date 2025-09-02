PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")
ENV(YDB_ADDITIONAL_LOG_CONFIGS="TX_TIERING:DEBUG")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(19)

SIZE(MEDIUM)

TEST_SRCS(
    test_s3.py
)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
    contrib/python/moto
    contrib/python/boto3
)

DEPENDS(
    ydb/apps/ydb
    contrib/python/moto/bin
)

END()
