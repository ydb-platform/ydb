PY3TEST()

OWNER(g:passport_infra)

TEST_SRCS(
    test.py
)

PEERDIR(
    contrib/python/requests
)

INCLUDE(${ARCADIA_ROOT}/library/recipes/tvmtool/recipe_with_default_cfg.inc)

END()
