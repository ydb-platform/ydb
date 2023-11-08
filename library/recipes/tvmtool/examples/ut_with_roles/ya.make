PY3TEST()

OWNER(g:passport_infra)

TEST_SRCS(
    test.py
)

PEERDIR(
    contrib/python/requests
)

INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe.inc)

USE_RECIPE(
    library/recipes/tvmtool/tvmtool
    library/recipes/tvmtool/examples/ut_with_roles/custom.cfg
    --with-roles-dir library/recipes/tvmtool/examples/ut_with_roles/roles
)

END()
