PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test_workload_logs.py
)

PEERDIR(
    ydb/tests/library/stability/utils
)

END()
