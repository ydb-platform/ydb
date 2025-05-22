DEPENDS(
    ydb/library/yql/tools/solomon_emulator/bin
    ydb/library/yql/tools/solomon_emulator/recipe
)

USE_RECIPE(
    ydb/library/yql/tools/solomon_emulator/recipe/solomon_recipe --shard my_project/my_cluster/my_service
)
