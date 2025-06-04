PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

TEST_SRCS(
    reconfig_state_storage_workload_test.py
    test_board_workload.py
    test_state_storage_workload.py
)

REQUIREMENTS(ram:32)

SIZE(MEDIUM)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(IAM_TOKEN="")

DEPENDS(
    ydb/apps/ydbd
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
    ydb/tests/stress/common
    ydb/tests/stress/reconfig_state_storage_workload/workload
)


END()
