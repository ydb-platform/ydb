PY3TEST()

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")

TEST_SRCS(
    test_tpch_import.py
)

PY_SRCS(
    base.py
)

SIZE(MEDIUM)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    contrib/python/boto3
    library/recipes/common
    ydb/tests/olap/common
)

DEPENDS(
    ydb/apps/ydb
    contrib/python/moto/bin
)

END()

RECURSE(
    large
)
