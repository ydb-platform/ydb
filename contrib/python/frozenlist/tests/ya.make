PY3TEST()

PEERDIR(
    contrib/python/frozenlist
)

TEST_SRCS(
    conftest.py
    test_frozenlist.py
)

NO_LINT()

END()
