PY3TEST()

PEERDIR(
    contrib/python/tinycss2
)

DATA(
    arcadia/contrib/python/tinycss2/tests/css-parsing-tests
)

ALL_PYTEST_SRCS()

NO_LINT()

END()
