PY3TEST()

PEERDIR(
    contrib/python/dotmap
)

SRCDIR(
    contrib/python/dotmap/py3
)

TEST_SRCS(
    dotmap/test.py
)

NO_LINT()

END()
