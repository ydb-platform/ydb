PY3TEST()

SIZE(SMALL)
TIMEOUT(15)

FORK_SUBTESTS()

NO_CHECK_IMPORTS()

TEST_SRCS(
    test_crash.py
)

PEERDIR(
    contrib/python/pytest
)

END()
