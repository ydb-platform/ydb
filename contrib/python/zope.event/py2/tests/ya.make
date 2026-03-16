PY2TEST()

PEERDIR(
    contrib/python/zope.event
)

SRCDIR(
    contrib/python/zope.event/py2/zope/event
)

TEST_SRCS(
    tests.py
)

NO_LINT()

END()
