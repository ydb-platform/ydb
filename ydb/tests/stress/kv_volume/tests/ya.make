PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/kv_volume/workload_keyvalue_volume")

DATA(
    arcadia/ydb/tests/stress/kv_volume/tests/configs
)

TEST_SRCS(
    test_workload.py
)

IF (NOT SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:1)
ELSE()
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/kv_volume
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)

END()
