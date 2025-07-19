PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_TEST_PATH="ydb/tests/stress/s3_backups/s3_backups")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/s3_backups
)

PEERDIR(
    ydb/tests/library
    ydb/tests/stress/common
    ydb/tests/library/stress
    ydb/tests/stress/s3_backups/workload
)

END()
