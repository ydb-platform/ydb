PY3TEST()

PEERDIR(
    contrib/python/pyaes
)

TEST_SRCS(
    # Depends on reference Crypto package
    # test-aes.py
    test-blockfeeder.py
)

NO_LINT()

END()
