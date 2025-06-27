IF (NOT WITH_VALGRIND)

PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(STRESS_TEST_UTILITY="ydb/tests/stress/show_create/view/show_create_view")

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
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

ENDIF()
