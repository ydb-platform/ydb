DEPENDS(
    ydb/tests/tools/nfs_recipe
    ydb/tests/tools/nfs_recipe/fake_nfs
)

USE_RECIPE(
    ydb/tests/tools/nfs_recipe/nfs_recipe
)
