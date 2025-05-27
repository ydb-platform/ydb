PY3TEST()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/fft/tests/__init__.py
    numpy/fft/tests/test_helper.py
    numpy/fft/tests/test_pocketfft.py
)

END()
