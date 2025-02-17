PY3TEST()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/testing/tests/__init__.py
    numpy/testing/tests/test_utils.py
)

END()
