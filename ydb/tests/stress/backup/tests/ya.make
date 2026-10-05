PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_TEST_PATH="ydb/tests/stress/backup/backup_stress")

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:1)
ELSE()
    REQUIREMENTS(ram:32 cpu:2)
ENDIF()
SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/backup
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/backup/workload
)

END()