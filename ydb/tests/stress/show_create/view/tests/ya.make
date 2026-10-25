
PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(STRESS_TEST_UTILITY="ydb/tests/stress/show_create/view/show_create_view")

TEST_SRCS(
    test_workload.py
)

IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
    REQUIREMENTS(ram:32 cpu:2)
ELSE()
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/show_create/view
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/show_create/view/workload
)

END()
