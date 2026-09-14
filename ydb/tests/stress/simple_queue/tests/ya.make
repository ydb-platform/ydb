PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)


SIZE(MEDIUM)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/simple_queue/workload
)


END()
