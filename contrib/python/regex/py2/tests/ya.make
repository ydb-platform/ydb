PY2TEST()

PEERDIR(
    contrib/python/regex
)

SRCDIR(contrib/python/regex/py2/regex_2)

TEST_SRCS(
    test_regex.py
)

NO_LINT()

END()
