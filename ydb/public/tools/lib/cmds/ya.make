PY23_LIBRARY()
PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/six
    library/python/testing/recipe
    ydb/tests/library
)

END()

RECURSE_FOR_TESTS(ut)
