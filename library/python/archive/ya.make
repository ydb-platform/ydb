PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/pathlib2
    contrib/python/python-libarchive
)

END()

RECURSE_FOR_TESTS(
    benchmark
    test
)
