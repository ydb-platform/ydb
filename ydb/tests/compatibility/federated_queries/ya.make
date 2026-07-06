PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_external_data_source.py
    test_external_data_source_secret.py
    test_solomon_external_source_write.py
)

SIZE(LARGE)
REQUIREMENTS(cpu:16)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)


DEPENDS(
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    contrib/python/boto3
    ydb/library/yql/tools/solomon_emulator/client
    ydb/tests/library
    ydb/tests/library/compatibility
)

END()
