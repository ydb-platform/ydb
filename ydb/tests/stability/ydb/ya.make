PY3TEST()

TEST_SRCS(
    test_stability.py
)

TIMEOUT(18000)
SIZE(LARGE)
TAG(ya:manual)

DATA(
    arcadia/ydb/tests/stability/resources
)

DEPENDS(
    ydb/tools/simple_queue
    ydb/tools/olap_workload
    ydb/tools/olap_workload_tiering
    ydb/tools/cfg/bin
    ydb/tests/tools/nemesis/driver
)

PEERDIR(
    ydb/tests/library
)

END()

