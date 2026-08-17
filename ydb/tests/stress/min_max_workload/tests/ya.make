PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/min_max_workload/min_max_workload")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/min_max_workload
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/common
)

END()
