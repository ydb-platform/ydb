PY2TEST()

NO_LINT()

PEERDIR(
    contrib/python/mongolock
    contrib/python/mongomock
)

SRCDIR(
    contrib/python/mongolock/py2
)

TEST_SRCS(
    test_mongolock.py
)

END()
