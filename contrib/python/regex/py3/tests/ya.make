PY3TEST()

PEERDIR(
    contrib/python/regex
)

SRCDIR(contrib/python/regex/py3/regex/tests)

TEST_SRCS(
    test_regex.py
)

NO_LINT()

END()
