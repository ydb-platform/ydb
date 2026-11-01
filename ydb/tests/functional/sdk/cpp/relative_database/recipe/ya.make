PY3_PROGRAM(recipe)

PY_SRCS(
    __main__.py
)

PEERDIR(
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/tests/library
)

END()
