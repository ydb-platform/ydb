PY2TEST()

PEERDIR(
    contrib/python/dotmap
)

SRCDIR(
    contrib/python/dotmap/py2
)

TEST_SRCS(
    dotmap/test.py
)

NO_LINT()

END()
