PY3TEST()

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/antiadblock/postgres_local/recipe/recipe.inc)

DEPENDS(
    antiadblock/postgres_local/recipe
)

TEST_SRCS(
    tests.py
)

PEERDIR(
    contrib/python/pq
)

NO_LINT()

END()
