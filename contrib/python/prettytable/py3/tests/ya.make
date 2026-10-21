PY3TEST()

PEERDIR(
    contrib/python/prettytable
    contrib/python/pytest-lazy-fixtures
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
