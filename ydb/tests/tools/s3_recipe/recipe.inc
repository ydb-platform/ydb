DEPENDS(
    contrib/python/moto/bin
    ydb/tests/tools/s3_recipe
)

USE_RECIPE(
    ydb/tests/tools/s3_recipe/s3_recipe
)
