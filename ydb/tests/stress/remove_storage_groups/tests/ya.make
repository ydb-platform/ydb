PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/remove_storage_groups/remove_storage_groups")

TEST_SRCS(
    test_workload.py
)

SIZE(MEDIUM)
REQUIREMENTS(ram:32 cpu:4)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/remove_storage_groups
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)


END()
