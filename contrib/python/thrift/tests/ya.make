PY3TEST()

PEERDIR(
    contrib/python/thrift
    contrib/python/tornado
)

DATA(
    arcadia/contrib/python/thrift/tests/keys
)

ALL_PYTEST_SRCS()

NO_LINT()

END()
