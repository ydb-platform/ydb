PY3TEST()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/typing/tests/__init__.py
    #numpy/typing/tests/test_isfile.py
    #numpy/typing/tests/test_runtime.py
    numpy/typing/tests/test_typing.py
)

END()
