INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(STRESS_TEST_UTILITY="ydb/tests/stress/reconfig_state_storage_workload/reconfig_state_storage_workload")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(IAM_TOKEN="")

TEST_SRCS(
    reconfig_state_storage_workload_test.py
)

REQUIREMENTS(ram:32 cpu:4)

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
