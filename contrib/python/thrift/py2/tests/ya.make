PY2TEST()

PEERDIR(
    contrib/python/thrift
    contrib/python/tornado
)

DATA(
    arcadia/contrib/python/thrift/py2/tests/keys
)

TEST_SRCS(
    test_socket.py
    test_sslsocket.py
    thrift_json.py
)

NO_LINT()

END()
