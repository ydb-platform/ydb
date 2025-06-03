PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/tests/stress/common
    ydb/tests/stress/s3_workload/workload
)


END()
