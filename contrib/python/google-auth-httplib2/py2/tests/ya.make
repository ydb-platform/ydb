PY2TEST()

PEERDIR(
    contrib/python/Flask
    contrib/python/google-auth-httplib2
    contrib/python/mock
    contrib/python/pytest-localserver
)

PY_SRCS(
    NAMESPACE tests
    compliance.py
)

TEST_SRCS(
    test_google_auth_httplib2.py
)

NO_LINT()

END()
