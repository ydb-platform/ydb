PY3TEST()

FORK_SUBTESTS()

PEERDIR(
    contrib/python/numpy/py3/tests
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/linalg/tests/__init__.py
    numpy/linalg/tests/test_deprecations.py
    numpy/linalg/tests/test_linalg.py
    numpy/linalg/tests/test_regression.py
    numpy/matrixlib/tests/__init__.py
    numpy/matrixlib/tests/test_defmatrix.py
    numpy/matrixlib/tests/test_interaction.py
    numpy/matrixlib/tests/test_masked_matrix.py
    numpy/matrixlib/tests/test_matrix_linalg.py
    numpy/matrixlib/tests/test_multiarray.py
    numpy/matrixlib/tests/test_numeric.py
    numpy/matrixlib/tests/test_regression.py
)

END()
