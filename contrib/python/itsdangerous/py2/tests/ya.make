PY2TEST()

PEERDIR(
    contrib/python/freezegun
    contrib/python/itsdangerous
)

TEST_SRCS(
    test_itsdangerous/__init__.py
    test_itsdangerous/test_compat.py
    test_itsdangerous/test_encoding.py
    test_itsdangerous/test_jws.py
    test_itsdangerous/test_serializer.py
    test_itsdangerous/test_signer.py
    test_itsdangerous/test_timed.py
    test_itsdangerous/test_url_safe.py
)

NO_LINT()

END()
