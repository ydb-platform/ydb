PY3TEST()

PEERDIR(
    contrib/python/pytest-pretty
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
