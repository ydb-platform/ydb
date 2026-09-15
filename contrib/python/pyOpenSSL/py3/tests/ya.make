PY3TEST()

PEERDIR(
    contrib/python/pyOpenSSL
    contrib/python/pytest-rerunfailures
    contrib/python/pretend
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
