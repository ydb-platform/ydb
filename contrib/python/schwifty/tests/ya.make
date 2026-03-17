PY3TEST()

PEERDIR(
    contrib/python/schwifty
    contrib/python/pydantic/pydantic-2
)

TEST_SRCS(
    test_bban.py
    test_bic.py
    test_checksum.py
    test_iban.py
    test_registry.py
)

NO_LINT()

END()
