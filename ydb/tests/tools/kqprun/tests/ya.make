PY3TEST()

DATA(
    arcadia/ydb/tests/tools/kqprun/tests/cfg
)

TEST_SRCS(
    test_kqprun_recipe.py
)

PEERDIR(
    ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
    ydb/tests/tools/kqprun
    ydb/tests/tools/kqprun/recipe
)

USE_RECIPE(
    ydb/tests/tools/kqprun/recipe/kqprun_recipe 
        --config ydb/tests/tools/kqprun/tests/cfg/config.conf
        --query ydb/tests/tools/kqprun/tests/cfg/create_tables.sql
        --query ydb/tests/tools/kqprun/tests/cfg/fill_tables.sql
)

SIZE(MEDIUM)

END()
