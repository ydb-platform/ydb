PY3TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/pytest
    contrib/python/cookies
)

TEST_SRCS(
    contrib/python/cookies/py3/test_cookies.py
)

NO_LINT()

END()
