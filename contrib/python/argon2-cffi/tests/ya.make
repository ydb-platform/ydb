PY3TEST()

PEERDIR(
    contrib/python/argon2-cffi
    contrib/python/hypothesis
)

TEST_SRCS(
    __init__.py
    test_legacy.py
    test_low_level.py
    test_password_hasher.py
    test_utils.py
)

NO_LINT()

END()
