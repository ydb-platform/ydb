PY2TEST()

PEERDIR(
    contrib/python/iso8601
)

SRCDIR(contrib/python/iso8601/py2/iso8601)

TEST_SRCS(
    test_iso8601.py
)

NO_LINT()

END()
