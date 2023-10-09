PY2TEST()

PEERDIR(
    contrib/python/pytz
)

SRCDIR(
    contrib/python/pytz/py2/pytz/tests
)

TEST_SRCS(
    test_docs.py
    test_lazy.py
    test_tzinfo.py
)

NO_LINT()

END()
