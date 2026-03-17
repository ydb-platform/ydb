PY3TEST()

PEERDIR(
    contrib/python/httpretty
    contrib/python/hammock
)

TEST_SRCS(
    test_hammock.py
)

NO_LINT()

END()
