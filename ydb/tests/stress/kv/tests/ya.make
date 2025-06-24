IF (NOT WITH_VALGRIND)
PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
)


END()

ENDIF()
