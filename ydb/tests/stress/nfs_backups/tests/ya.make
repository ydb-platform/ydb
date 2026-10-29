PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_TEST_PATH="ydb/tests/stress/nfs_backups/nfs_backups")

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
    ydb/tests/stress/nfs_backups
)

PEERDIR(
    ydb/tests/library
    ydb/tests/stress/common
    ydb/tests/library/stress
    ydb/tests/stress/nfs_backups/workload
)

END()
