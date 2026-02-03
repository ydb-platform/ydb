IF (NOT WITH_VALGRIND)
PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="ydb/tests/stress/streaming/streaming")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/streaming
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/streaming/workload
)


END()

ENDIF()
