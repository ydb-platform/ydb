PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/vector_workload/workload_vector")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(LARGE)
TAG(ya:fat)
TIMEOUT(1200)

DEPENDS(
    ydb/tests/stress/vector_workload
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)

END()
