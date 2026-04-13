PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_TEST_PATH="ydb/tests/stress/system_tablet_backup/system_tablet_backup")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/system_tablet_backup
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/system_tablet_backup/workload
)

END()
