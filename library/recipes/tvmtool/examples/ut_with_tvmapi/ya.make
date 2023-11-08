PY3TEST()

OWNER(g:passport_infra)

TEST_SRCS(test.py)

PEERDIR(
    library/python/deprecated/ticket_parser2
)

# common usage
INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmapi/recipe.inc)

# tvmtool for connoisseurs
INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe.inc)
USE_RECIPE(
    library/recipes/tvmtool/tvmtool
    library/recipes/tvmtool/examples/ut_with_tvmapi/tvmtool.conf
    --with-tvmapi
)

END()
