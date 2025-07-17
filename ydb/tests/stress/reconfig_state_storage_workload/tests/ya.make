PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(STRESS_TEST_UTILITY="ydb/tests/stress/reconfig_state_storage_workload/reconfig_state_storage_workload")

TEST_SRCS(
    reconfig_state_storage_workload_test.py
    test_board_workload.py
    test_scheme_board_workload.py
    test_state_storage_workload.py
)

REQUIREMENTS(ram:32)

SIZE(MEDIUM)

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(IAM_TOKEN="")

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/reconfig_state_storage_workload
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
    ydb/tests/library/stress
    ydb/tests/stress/common
    ydb/tests/stress/reconfig_state_storage_workload/workload
)


END()
