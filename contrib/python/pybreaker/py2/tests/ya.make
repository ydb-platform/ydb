PY2TEST()

PEERDIR(
    contrib/python/mock
    contrib/python/pybreaker
    contrib/python/fakeredis
    contrib/python/tornado
)

SRCDIR(
    contrib/python/pybreaker/py2
)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
