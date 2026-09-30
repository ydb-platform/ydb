ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

DEPENDS(
    ydb/apps/ydbd
    ydb/tests/functional/security/oidc/recipe
)

USE_RECIPE(
    ydb/tests/functional/security/oidc/recipe/oidc_recipe
)

# Use the same Docker-capable CI environment as the shared Compose recipe.
INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/large.inc)

REQUIREMENTS(
    cpu:all
    dns:dns64
    container:4467981730
)
