PY3TEST()

PEERDIR(
    contrib/python/iso8601
    contrib/python/hypothesis
    contrib/python/pytz
)

SRCDIR(contrib/python/iso8601/py3/iso8601)

TEST_SRCS(
    test_iso8601.py
)

NO_LINT()

END()
