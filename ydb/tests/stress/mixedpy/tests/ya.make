PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/mixedpy/workload_mixed")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(LARGE)
TAG(
    ya:fat
)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/mixedpy
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
)


END()
