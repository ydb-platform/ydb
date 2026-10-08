PY3_PROGRAM(solomon_recipe)

STYLE_PYTHON()

PY_SRCS(__main__.py)

PEERDIR(
    library/python/port_manager
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
)

END()
