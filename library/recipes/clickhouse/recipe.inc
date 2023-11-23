INCLUDE(${ARCADIA_ROOT}/library/recipes/clickhouse/recipe/recipe.inc)

DEPENDS(library/recipes/clickhouse)

USE_RECIPE(library/recipes/clickhouse/clickhouse-recipe "${CLICKHOUSE_RECIPE_OPTS}")
