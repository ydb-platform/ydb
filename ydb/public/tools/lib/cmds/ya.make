PY23_LIBRARY()
PY_SRCS(
    __init__.py
)

PEERDIR(
    ydb/tests/library
    library/python/testing/recipe
)

END()

RECURSE_FOR_TESTS(ut)
