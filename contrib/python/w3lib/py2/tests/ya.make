PY2TEST()

PEERDIR(
    contrib/python/w3lib
)

TEST_SRCS(
    __init__.py
    test_encoding.py
    test_form.py
    test_html.py
    test_http.py
    test_url.py
)

NO_LINT()

END()
