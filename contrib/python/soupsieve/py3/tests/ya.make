PY3TEST()

PEERDIR(
    contrib/python/beautifulsoup4
    contrib/python/soupsieve
)

ALL_PYTEST_SRCS(RECURSIVE)

NO_LINT()

END()
