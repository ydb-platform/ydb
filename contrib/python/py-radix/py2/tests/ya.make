PY2TEST()

PEERDIR(
    contrib/python/py-radix
)

DATA(
    arcadia/contrib/python/py-radix/py2/tests/data
)

TEST_SRCS(
    __init__.py
    test_compat.py
    test_regression.py
)

NO_LINT()

END()
