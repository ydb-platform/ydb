PY3TEST()

PEERDIR(
    contrib/python/wurlitzer
)

SRCDIR(contrib/python/wurlitzer)

TEST_SRCS(
    test.py
)

NO_LINT()

END()
