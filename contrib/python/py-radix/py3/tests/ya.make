PY3TEST()

PEERDIR(
    contrib/python/py-radix
)

DATA(
    arcadia/contrib/python/py-radix/py3/tests/data
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
