PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/topic_balancing/workload_topic_balancing")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:8)
SIZE(LARGE)
TAG(ya:fat)
TIMEOUT(1200)

DEPENDS(
    ydb/tests/stress/topic_balancing
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)

END()
