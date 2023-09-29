PY3TEST()

PEERDIR(
    contrib/python/freezegun
    contrib/python/itsdangerous
)

TEST_SRCS(
    __init__.py
    test_encoding.py
    test_serializer.py
    test_signer.py
    test_timed.py
    test_url_safe.py
)

NO_LINT()

REQUIREMENTS(ram:10)

END()
