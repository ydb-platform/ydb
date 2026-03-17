PY3TEST()

PEERDIR(
    contrib/python/pytest-freezegun
)

NO_LINT()

TEST_SRCS(
    test_freezegun.py
)

END()
