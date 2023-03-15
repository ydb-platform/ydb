PY23_LIBRARY()

PY_SRCS(__init__.py)

PEERDIR(
    contrib/python/python-libarchive
)

END()

RECURSE_FOR_TESTS(
    benchmark
    test
)
