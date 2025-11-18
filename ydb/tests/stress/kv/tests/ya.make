IF (NOT WITH_VALGRIND)
PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/kv/workload_kv")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(LARGE)

DEPENDS(
    ydb/tests/stress/kv
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)


END()

ENDIF()
