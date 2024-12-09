PY3TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/pyOpenSSL
    contrib/python/flaky
    contrib/python/pretend
)

TEST_SRCS(
    __init__.py
    conftest.py
    test_crypto.py
    test_debug.py
    test_rand.py
    test_ssl.py
    test_util.py
    util.py
)

NO_LINT()

END()
