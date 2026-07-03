PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(14)

TEST_SRCS(
    test_fulltext_index.py
    test_vector_index.py
    test_unique_index.py
    test_json_index.py
    test_bloom_filter_index.py
)

SIZE(LARGE)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:16)
ELSE()
    REQUIREMENTS(cpu:4)
ENDIF()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/compatibility
)

END()
