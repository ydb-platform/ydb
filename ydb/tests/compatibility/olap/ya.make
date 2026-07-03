PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_bloom_index.py
    test_min_max_index.py
    test_rename_table.py
    test_compression.py
    test_encoding.py
)

SIZE(LARGE)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:8)
ELSE()
    REQUIREMENTS(cpu:4)
ENDIF()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)


DEPENDS(
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    contrib/python/boto3
    ydb/tests/library
    ydb/tests/library/compatibility
)

END()
