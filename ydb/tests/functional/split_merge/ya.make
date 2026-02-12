PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(45)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TEST_SRCS(
    conftest.py
    test_split_merge.py
    test_split_merge_by_load.py
)

PEERDIR(
    ydb/tests/sql/lib
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
