PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/mongolock
    contrib/python/mongomock
)

SRCDIR(
    contrib/python/mongolock/py3
)

TEST_SRCS(
    test_mongolock.py
)

END()
