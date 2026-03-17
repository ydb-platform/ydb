PY3TEST()

PEERDIR(
    contrib/python/uritemplate
)

ALL_PYTEST_SRCS()

DATA(
    arcadia/contrib/python/uritemplate/py3/tests
)

NO_LINT()

END()
