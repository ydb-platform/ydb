PY3TEST()

TEST_SRCS(
    test_workload_base.py
)

SIZE(SMALL)

PEERDIR(
    ydb/tests/stress/common
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
)

END()
