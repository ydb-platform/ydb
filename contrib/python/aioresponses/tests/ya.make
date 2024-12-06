PY3TEST()

SUBSCRIBER(g:python-contrib)

NO_LINT()

PEERDIR(
    contrib/python/aioresponses
)

ALL_PYTEST_SRCS()

END()
