PY3TEST()

DATA(
    arcadia/ydb/tests/tools/kqprun/tests/cfg
)

TEST_SRCS(
    test_kqprun_recipe.py
)

PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
    ydb/tests/tools/kqprun
    ydb/tests/tools/kqprun/recipe
)

USE_RECIPE(
    ydb/tests/tools/kqprun/recipe/kqprun_recipe --query ydb/tests/tools/kqprun/tests/cfg/create_tables.sql --query ydb/tests/tools/kqprun/tests/cfg/fill_tables.sql --config ydb/tests/tools/kqprun/tests/cfg/config.conf
)

TIMEOUT(10)
SIZE(MEDIUM)

END()
