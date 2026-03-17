PY3TEST()

PEERDIR(
    contrib/python/aiosmtpd
)

SRCDIR(contrib/python/aiosmtpd/aiosmtpd/qa)

TEST_SRCS(
    test_1testsuite.py
)

END()
