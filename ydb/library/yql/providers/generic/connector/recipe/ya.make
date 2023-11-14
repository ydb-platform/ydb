PY3_PROGRAM()

STYLE_PYTHON()

PY_SRCS(__main__.py)

PEERDIR(
    contrib/python/Jinja2
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
)

END()
