PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_followers.py
    test_compatibility.py
    test_stress.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:all)
REQUIREMENTS(ram:all)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

DEPENDS(
    ydb/apps/ydb
    ydb/tests/library/compatibility
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/library
    ydb/tests/stress/simple_queue/workload
)

END()