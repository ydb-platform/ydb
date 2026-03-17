PY3TEST()

REQUIREMENTS(network:full)

PEERDIR(
    contrib/python/pycbrf
    contrib/python/pytest-datafixtures
)

DATA(
    arcadia/contrib/python/pycbrf/tests
)

TEST_SRCS(
    test_banks.py
    test_rates.py
)

NO_LINT()

END()
