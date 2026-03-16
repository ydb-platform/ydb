PY3TEST()

PEERDIR(
    contrib/python/httplib2
    contrib/python/requests
    contrib/python/wsgi-intercept
)

ENV(
    WSGI_INTERCEPT_SKIP_NETWORK=true
)

SRCDIR(contrib/python/wsgi-intercept/wsgi_intercept/tests)

TEST_SRCS(
    __init__.py
    install.py
    test_http_client.py
    test_httplib2.py
    test_interceptor.py
    test_module_interceptor.py
    test_requests.py
    test_response_headers.py
    test_urllib.py
    test_urllib3.py
    test_wsgi_compliance.py
    test_wsgiref.py
    wsgi_app.py
)

NO_LINT()

END()
