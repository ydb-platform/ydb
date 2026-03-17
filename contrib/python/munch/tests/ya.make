PY3TEST()

PEERDIR(
    contrib/python/munch
    contrib/python/PyYAML
)

DATA(arcadia/contrib/python/munch)

ALL_PYTEST_SRCS()

NO_LINT()

END()
