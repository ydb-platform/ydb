PY3TEST()

PEERDIR(
    contrib/python/numpy/py3/tests
)

DATA(
    arcadia/contrib/python/numpy/py3/numpy
)

NO_LINT()

SRCDIR(contrib/python/numpy/py3)

TEST_SRCS(
    numpy/random/tests/__init__.py
    numpy/random/tests/data/__init__.py
    numpy/random/tests/test_direct.py
    numpy/random/tests/test_extending.py
    numpy/random/tests/test_generator_mt19937.py
    numpy/random/tests/test_generator_mt19937_regressions.py
    numpy/random/tests/test_random.py
    numpy/random/tests/test_randomstate.py
    numpy/random/tests/test_randomstate_regression.py
    numpy/random/tests/test_regression.py
    numpy/random/tests/test_seed_sequence.py
    numpy/random/tests/test_smoke.py
)

END()
