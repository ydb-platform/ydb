PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/testshard_workload/workload_testshard")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/stress/testshard_workload
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/common
)

END()
