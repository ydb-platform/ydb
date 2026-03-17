PY2TEST()

PEERDIR(
    contrib/python/retrying
)

TEST_SRCS(
    test_retrying.py
)

NO_LINT()

END()
