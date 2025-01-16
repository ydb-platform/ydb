PY2TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/pytest-localserver
    contrib/python/requests
)

TEST_SRCS(
    test_http.py
    test_https.py
    test_smtp.py
)

NO_LINT()

END()
