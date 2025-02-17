PY3TEST()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/compat/tests/__init__.py
    numpy/compat/tests/test_compat.py
)

END()
