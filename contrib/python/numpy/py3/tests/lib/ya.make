PY3TEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/python/numpy/py3/tests
)

DATA(
    arcadia/contrib/python/numpy/py3/numpy
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/lib/tests/__init__.py
    numpy/lib/tests/test__datasource.py
    numpy/lib/tests/test__iotools.py
    numpy/lib/tests/test__version.py
    numpy/lib/tests/test_arraypad.py
    numpy/lib/tests/test_arraysetops.py
    numpy/lib/tests/test_arrayterator.py
    numpy/lib/tests/test_financial_expired.py
    numpy/lib/tests/test_format.py
    numpy/lib/tests/test_function_base.py
    numpy/lib/tests/test_histograms.py
    numpy/lib/tests/test_index_tricks.py
    numpy/lib/tests/test_io.py
    numpy/lib/tests/test_loadtxt.py
    numpy/lib/tests/test_mixins.py
    numpy/lib/tests/test_nanfunctions.py
    numpy/lib/tests/test_packbits.py
    numpy/lib/tests/test_polynomial.py
    numpy/lib/tests/test_recfunctions.py
    numpy/lib/tests/test_regression.py
    numpy/lib/tests/test_shape_base.py
    numpy/lib/tests/test_stride_tricks.py
    numpy/lib/tests/test_twodim_base.py
    numpy/lib/tests/test_type_check.py
    numpy/lib/tests/test_ufunclike.py
    numpy/lib/tests/test_utils.py
)

END()
