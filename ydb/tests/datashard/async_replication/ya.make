PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(20)
SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

TEST_SRCS(
    test_async_replication.py
    test_consistency.py
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/datashard/lib
    ydb/tests/library
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
