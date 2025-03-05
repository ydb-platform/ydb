PY3TEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/ma/tests/__init__.py
    numpy/ma/tests/test_core.py
    numpy/ma/tests/test_deprecations.py
    numpy/ma/tests/test_extras.py
    numpy/ma/tests/test_mrecords.py
    numpy/ma/tests/test_old_ma.py
    numpy/ma/tests/test_regression.py
    numpy/ma/tests/test_subclassing.py
)

END()
