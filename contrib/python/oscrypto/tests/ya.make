PY3TEST()

PEERDIR(
    contrib/python/oscrypto
)

NO_LINT()

ENV(OPENSSL_SKIP_INTERNET_TESTS=true)
ENV(OSCRYPTO_SKIP_INTERNET_TESTS=true)

PY_SRCS(
    __init__.py
)

TEST_SRCS(
    _https_client.py
    _socket_proxy.py
    _socket_server.py
    _unittest_compat.py
    exception_context.py
    unittest_data.py

    test_asymmetric.py
    # test_init.py  # breaks due to our custom packaging
    test_kdf.py
    test_keys.py
    test_symmetric.py
    # test_tls.py  # requires external network to test against public services
    test_trust_list.py
)
# note: test files should be taken manually since pypi whl used by yamaker does not contain them
# do not forget to run ./fix_test_import.sh to update import of fixture data

END()
