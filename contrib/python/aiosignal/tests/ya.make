PY3TEST()

PEERDIR(
    contrib/python/aiosignal
)

TEST_SRCS(
    conftest.py
    test_signals.py
)

NO_LINT()

END()
