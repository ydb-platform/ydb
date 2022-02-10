ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

DEPENDS(
    ydb/apps/ydbd
    ydb/public/tools/ydb_recipe
)

USE_RECIPE(
    ydb/public/tools/ydb_recipe/ydb_recipe --suppress-version-check
)
