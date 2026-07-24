PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_chaos_target.py
)

PEERDIR(
    ydb/tests/stability/nemesis
    contrib/python/PyYAML
    contrib/python/pytest
)

END()
