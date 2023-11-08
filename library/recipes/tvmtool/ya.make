PY3_PROGRAM()

PY_SRCS(__main__.py)

PEERDIR(
    contrib/python/requests
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
)

END()

RECURSE_FOR_TESTS(
    examples
)
