PY3TEST()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/tests/__init__.py
    numpy/tests/test__all__.py
    #numpy/tests/test_ctypeslib.py
    numpy/tests/test_lazyloading.py
    numpy/tests/test_matlib.py
    numpy/tests/test_numpy_config.py
    numpy/tests/test_numpy_version.py
    numpy/tests/test_public_api.py
    numpy/tests/test_reloading.py
    #numpy/tests/test_scripts.py
    numpy/tests/test_warnings.py
)

END()
