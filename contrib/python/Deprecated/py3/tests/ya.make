PY3TEST()

PEERDIR(
    contrib/python/Deprecated
    contrib/python/setuptools
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
