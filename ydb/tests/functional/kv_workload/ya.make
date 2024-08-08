IF (NOT SANITIZER_TYPE AND NOT WITH_VALGRIND)
PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test_kv_workload.py
)

REQUIREMENTS(
    cpu:1
    ram:32
)

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

DEPENDS(
    ydb/apps/ydbd
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
)


END()

ENDIF()
