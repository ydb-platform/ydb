PY3TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/prettytable
    contrib/python/pytest-lazy-fixtures
)

TEST_SRCS(
    test_colortable.py
    test_prettytable.py
)

NO_LINT()

END()
