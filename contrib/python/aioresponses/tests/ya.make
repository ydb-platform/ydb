SUBSCRIBER(g:python-contrib)

PY3TEST()

NO_LINT()

PEERDIR(
    contrib/python/aioresponses
)

TEST_SRCS(
    base.py
    test_aioresponses.py
    test_compat.py
)

END()
