PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_chaos_target.py
    test_weighted_scheduler.py
    test_recovery_probe.py
)

PEERDIR(
    ydb/tests/stability/nemesis
    contrib/python/PyYAML
    contrib/python/pytest
)

END()
