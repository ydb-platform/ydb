DEPENDS(
    ydb/public/tools/federation_recipe
    ydb/public/tools/federation_recipe/bin
    ydb/public/tools/federation_discovery_recipe
    ydb/public/tools/federation_discovery_recipe/bin
    ydb/apps/ydbd
)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_TEST_DISABLE_MLOCK=1)

USE_RECIPE(
    ydb/public/tools/federation_recipe/federation_recipe
)

USE_RECIPE(
    ydb/public/tools/federation_discovery_recipe/federation_discovery_recipe
)
