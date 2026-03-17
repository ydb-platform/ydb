PY3TEST()

PEERDIR(
    contrib/python/asgiref
    contrib/python/blinker
    contrib/python/Flask-Login
    contrib/python/semantic-version
)

TEST_SRCS(
    test_login.py
)

NO_LINT()

END()
