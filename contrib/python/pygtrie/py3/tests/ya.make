PY3TEST()

PEERDIR(
    contrib/python/pygtrie
)

SRCDIR(contrib/python/pygtrie/py3)

TEST_SRCS(
    test.py
)

NO_LINT()

END()
