PY3TEST()

PEERDIR(
    contrib/python/pyre2
)

TEST_SRCS(
    test_charliterals.py
    test_re.py
)

NO_LINT()

END()
