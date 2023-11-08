PY3TEST()

OWNER(g:passport_infra)

TEST_SRCS(test.py)

PEERDIR(
    library/python/tvmauth
)

# start tvm-api
INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmapi/recipe.inc)

# start tirole
INCLUDE(${ARCADIA_ROOT}/library/recipes/tirole/recipe.inc)
USE_RECIPE(
    library/recipes/tirole/tirole
    --roles-dir library/recipes/tvmtool/examples/ut_with_tvmapi_and_tirole/roles_dir
)

# tvmtool for connoisseurs
INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe.inc)
USE_RECIPE(
    library/recipes/tvmtool/tvmtool
    library/recipes/tvmtool/examples/ut_with_tvmapi_and_tirole/tvmtool.conf
    --with-tvmapi
    --with-tirole
)

END()
