PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_local_kikimr.py
    test_bridge_schedule_profile.py
)

PEERDIR(
    ydb/tests/stability/nemesis
    contrib/python/pytest
)

END()
