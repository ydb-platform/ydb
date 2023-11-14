IF (NOT OS_WINDOWS)

PY3_PROGRAM(clickhouse-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    library/recipes/clickhouse/recipe
    library/recipes/common
)

END()

RECURSE(
    recipe
)

ENDIF()
