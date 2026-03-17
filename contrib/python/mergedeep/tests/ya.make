PY3TEST()

PEERDIR(
    contrib/python/mergedeep
)

SRCDIR(contrib/python/mergedeep/mergedeep)

TEST_SRCS(
    test_mergedeep.py
)

NO_LINT()

END()
