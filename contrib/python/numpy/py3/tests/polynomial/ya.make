PY3TEST()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/polynomial/tests/__init__.py
    numpy/polynomial/tests/test_chebyshev.py
    numpy/polynomial/tests/test_classes.py
    numpy/polynomial/tests/test_hermite.py
    numpy/polynomial/tests/test_hermite_e.py
    numpy/polynomial/tests/test_laguerre.py
    numpy/polynomial/tests/test_legendre.py
    numpy/polynomial/tests/test_polynomial.py
    numpy/polynomial/tests/test_polyutils.py
    numpy/polynomial/tests/test_printing.py
    numpy/polynomial/tests/test_symbol.py
)

END()
