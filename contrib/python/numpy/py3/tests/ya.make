PY3_LIBRARY()

VERSION(1.26.4)

LICENSE(BSD-3-Clause)

PEERDIR(
    contrib/python/numpy
    contrib/python/hypothesis
    contrib/python/typing-extensions
    contrib/python/pytz
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

PY_SRCS(
    TOP_LEVEL
    numpy/conftest.py
    numpy/_core/tests/_locales.py
)

END()

RECURSE_FOR_TESTS(
    _core
    # distutils
    # f2py
    fft
    lib
    linalg
    ma
    # matrixlib - merged with linalg
    polynomial
    random
    testing
    tests
    typing
)
