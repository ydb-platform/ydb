PY3TEST()

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(MOTO_SERVER_PATH="contrib/python/moto/bin/moto_server")

TEST_SRCS(
    test_simple_table.py
    test_tpch_import.py
    test_types_and_formats.py
)

FORK_SUBTESTS()
SPLIT_FACTOR(100)

PY_SRCS(
    base.py
)

SIZE(LARGE)

TAG(ya:fat)

TIMEOUT(900)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    contrib/python/boto3
    contrib/python/pyarrow
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
