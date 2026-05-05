PY23_LIBRARY()

PY_SRCS(
    conftest.py
)

PEERDIR(
    library/python/pytest
)

STYLE_PYTHON()

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
    tests
)
