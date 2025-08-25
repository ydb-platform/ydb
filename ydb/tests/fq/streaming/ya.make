PY3TEST()

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

TEST_SRCS(
    test_streaming.py
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
    library/recipes/common
    ydb/tests/olap/common
    ydb/tests/tools/datastreams_helpers
)

TIMEOUT(60)
DEPENDS(
    ydb/apps/ydb
)

END()
