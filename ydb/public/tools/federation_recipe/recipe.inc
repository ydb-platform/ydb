DEPENDS(
    ydb/public/tools/federation_recipe
    ydb/apps/ydbd
)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

USE_RECIPE(
    ydb/public/tools/federation_recipe/federation_recipe
)
