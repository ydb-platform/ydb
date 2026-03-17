PY3TEST()

PEERDIR(
    contrib/python/requests
    contrib/python/pytest-httpretty
    contrib/python/pytest
)

ALL_PYTEST_SRCS(RECURSIVE)

END()
