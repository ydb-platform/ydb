PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/log/workload_log")

TEST_SRCS(
    test_workload.py
)

SIZE(MEDIUM)
REQUIREMENTS(ram:32)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/log
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)


END()
