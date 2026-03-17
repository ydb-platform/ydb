PY2TEST()

PEERDIR(
    contrib/python/fallocate
)

TEST_SRCS(
    test_fallocate.py
)

NO_LINT()

END()
