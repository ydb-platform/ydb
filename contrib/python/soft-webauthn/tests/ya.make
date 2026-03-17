PY3TEST()

PEERDIR(
    contrib/python/Flask
    contrib/python/soft-webauthn
)

TEST_SRCS(
    __init__.py
    example_server.py
    test_class.py
    test_example.py
    test_interop.py
)

NO_LINT()

END()
