PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_TEST_PATH="ydb/tests/stress/s3_backups/s3_backups")

TEST_SRCS(
    test_workload.py
    test_table_checksums.py
)

REQUIREMENTS(ram:32 cpu:1)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/s3_backups
)

PEERDIR(
    contrib/python/grpcio
    ydb/tests/library
    ydb/tests/library/backup
    ydb/tests/oss/ydb_sdk_import
    ydb/tests/stress/common
    ydb/tests/library/stress
    ydb/tests/stress/s3_backups/workload
)

END()
