PY2TEST()

PEERDIR(
    contrib/python/mock
    contrib/python/schedule
)

TEST_SRCS(
    test_schedule.py
)

NO_LINT()

END()
