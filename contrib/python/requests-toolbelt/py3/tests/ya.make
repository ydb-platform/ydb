PY3TEST()

PEERDIR(
    contrib/python/requests-toolbelt
    contrib/python/betamax
    contrib/python/pyOpenSSL
)

DATA(
    arcadia/contrib/python/requests-toolbelt/py3/tests/cassettes
    arcadia/contrib/python/requests-toolbelt/py3/tests/data
)

TEST_SRCS(
    __init__.py
    test_auth_handler.py
    test_auth.py
    test_downloadutils.py
    test_dump.py
    test_fingerprintadapter.py
    test_forgetfulcookiejar.py
    test_formdata.py
    test_host_header_ssl_adapter.py
    test_multipart_decoder.py
    test_multipart_encoder.py
    test_multipart_monitor.py
    test_proxy_digest_auth.py
    test_sessions.py
    test_socket_options_adapter.py
    test_source_adapter.py
    test_ssladapter.py
    test_streaming_iterator.py
    test_user_agent.py
    # test_x509_adapter.py
)

NO_LINT()

END()
