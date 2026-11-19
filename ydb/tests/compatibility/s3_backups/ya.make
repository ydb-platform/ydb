PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_export_import_s3.py
    test_table_checksum.py
)

SIZE(LARGE)
REQUIREMENTS(ram:16 cpu:4)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)


DEPENDS(
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    contrib/python/boto3
    library/python/port_manager
    ydb/tests/library
    ydb/tests/library/compatibility
)

END()
