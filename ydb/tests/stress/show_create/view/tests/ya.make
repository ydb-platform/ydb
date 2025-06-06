IF (NOT WITH_VALGRIND)

PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
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
    ydb/apps/ydbd
    ydb/apps/ydb
    ydb/tests/stress/show_create/view
)

PEERDIR(
    ydb/tests/library
    ydb/tests/stress/show_create/view/workload
)

END()

ENDIF()
