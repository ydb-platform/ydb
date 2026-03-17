PY3TEST()

PEERDIR(
    contrib/python/freezegun
    contrib/python/python-json-logger
    contrib/python/tzdata
)

ALL_PYTEST_SRCS()

NO_LINT()

END()
