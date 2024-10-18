PY2TEST()

SUBSCRIBER(g:python-contrib)

PEERDIR(
    contrib/python/hyperlink
)

NO_LINT()

SRCDIR(
    contrib/python/hyperlink/py2/hyperlink/test
)

TEST_SRCS(
    __init__.py
    common.py
    test_common.py
    test_decoded_url.py
    test_parse.py
    test_scheme_registration.py
    test_socket.py
    test_url.py
)

END()
