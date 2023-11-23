PY23_LIBRARY()

PY_SRCS(__init__.py)

PEERDIR(contrib/python/six)

END()

RECURSE_FOR_TESTS(
    ut
)
