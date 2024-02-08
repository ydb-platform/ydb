PY3TEST()

PEERDIR(
    contrib/python/websocket-client
)

DATA(
    arcadia/contrib/python/websocket-client/py3/websocket/tests/data
)

SRCDIR(
    contrib/python/websocket-client/py3/websocket/tests
)

TEST_SRCS(
    __init__.py
    test_abnf.py
    test_app.py
    test_cookiejar.py
    test_http.py
    test_url.py
    test_websocket.py
)

NO_LINT()

END()
