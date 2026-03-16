PY2TEST()

PEERDIR(
    contrib/python/pygtrie
)

SRCDIR(contrib/python/pygtrie/py2)

TEST_SRCS(
    test.py
)

NO_LINT()

END()
