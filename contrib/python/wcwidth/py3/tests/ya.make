PY3TEST()

PEERDIR(
    contrib/python/wcwidth
)

DATA(
    arcadia/contrib/python/wcwidth/py3/tests
)

ALL_PYTEST_SRCS()

NO_LINT()

END()
