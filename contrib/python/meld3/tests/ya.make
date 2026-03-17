PY3TEST()

PEERDIR(contrib/python/meld3)

SRCDIR(contrib/python/meld3/meld3)

TEST_SRCS(
    test_meld3.py
)

NO_LINT()

END()
