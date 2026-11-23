PY3TEST()

TEST_SRCS(
    test_runner.py
)

PEERDIR(
    ydb/tests/stress/cdc/workload
)

SIZE(SMALL)

END()
