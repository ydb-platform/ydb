PY2TEST()

SUBSCRIBER(g:python-contrib)

SRCDIR(contrib/deprecated/python/enum34/enum)

TEST_SRCS(
    test.py
)

PEERDIR(
    contrib/deprecated/python/enum34
)

NO_LINT()

END()
