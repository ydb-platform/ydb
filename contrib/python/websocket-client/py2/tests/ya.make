PY2TEST()

PEERDIR(
    contrib/python/PySocks
    contrib/python/websocket-client
)

DATA(
    arcadia/contrib/python/websocket-client/py2/websocket/tests/data
)

SRCDIR(
    contrib/python/websocket-client/py2/websocket/tests
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
