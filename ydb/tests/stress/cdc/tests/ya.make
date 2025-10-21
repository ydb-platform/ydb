PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="ydb/tests/stress/cdc/cdc")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32)
SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/cdc
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/cdc/workload
)

END()
