PY3TEST()

PEERDIR(
    contrib/python/shortuuid
)

SRCDIR(contrib/python/shortuuid/shortuuid)

TEST_SRCS(
    test_shortuuid.py
)

NO_LINT()

END()
