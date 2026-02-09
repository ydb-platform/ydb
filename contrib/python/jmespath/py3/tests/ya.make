PY3TEST()

PEERDIR(
    contrib/python/jmespath
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
