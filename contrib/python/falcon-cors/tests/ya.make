PY3TEST()

PEERDIR(
    contrib/python/falcon
    contrib/python/six
    contrib/python/requests
    contrib/python/mock
    contrib/python/falcon-cors
)

TEST_SRCS(
    test_cors.py
)

NO_LINT()

END()
