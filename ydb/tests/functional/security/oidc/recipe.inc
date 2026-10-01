ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

DEPENDS(
    library/recipes/docker_compose/bin
    ydb/apps/ydbd
    ydb/tests/functional/security/oidc/recipe
)

DATA(
    arcadia/ydb/tests/functional/security/oidc/recipe/docker-compose.yml
    arcadia/ydb/tests/functional/security/oidc/recipe/keycloak.d
    arcadia/ydb/tests/functional/security/oidc/recipe/test-env.json
    arcadia/ydb/tests/functional/security/oidc/recipe/ydb-config.yaml
)

USE_RECIPE(
    ydb/tests/functional/security/oidc/recipe/oidc_recipe
)

SIZE(MEDIUM)

REQUIREMENTS(
    cpu:4
    dns:dns64
)
