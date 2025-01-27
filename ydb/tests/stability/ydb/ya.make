PY3TEST()

TEST_SRCS(
    test_stability.py
)

SIZE(LARGE)
TAG(ya:manual)

DATA(
    arcadia/ydb/tests/stability/resources
)

DEPENDS(
    ydb/tests/stress/simple_queue
    ydb/tests/stress/olap_workload
    ydb/tests/stress/statistics_workload
    ydb/tools/cfg/bin
    ydb/tests/tools/nemesis/driver
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/wardens
)

END()

