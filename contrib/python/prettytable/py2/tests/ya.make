PY2TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/prettytable
)

TEST_SRCS(
    prettytable_test.py
)

NO_LINT()

END()
