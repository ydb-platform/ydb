PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_export_s3.py
    test_followers.py
    test_compatibility.py
    test_stress.py
    test_rolling.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:all)
REQUIREMENTS(ram:all)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/library
    ydb/tests/stress/simple_queue/workload
    ydb/tests/library/compatibility
)

END()
