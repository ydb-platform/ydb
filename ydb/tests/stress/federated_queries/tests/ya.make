PY3TEST()

STYLE_PYTHON()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="ydb/tests/stress/federated_queries/federated_queries")

TEST_SRCS(
    test_workload.py
)

IF (NOT SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:3)
ELSE()
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/federated_queries
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/federated_queries/workload
)

END()
