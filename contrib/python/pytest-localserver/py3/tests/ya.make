PY3TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/pytest-localserver
    contrib/python/requests
    contrib/python/aiosmtpd
)

TEST_SRCS(
    test_http.py
    test_https.py
    test_smtp.py
    test_version.py
)

NO_LINT()

END()
