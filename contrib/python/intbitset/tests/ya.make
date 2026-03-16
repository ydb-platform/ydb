PY3TEST()

FORK_SUBTESTS()

PEERDIR(
    contrib/python/intbitset
)

TEST_SRCS(
    test_intbitset.py
)

NO_LINT()

END()
