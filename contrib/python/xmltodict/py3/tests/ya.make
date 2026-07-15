PY3TEST()

PEERDIR(
    contrib/python/xmltodict
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
