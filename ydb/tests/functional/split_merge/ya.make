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
    test_split_merge.py
)

PEERDIR(
    ydb/tests/sql/lib
    ydb/tests/library
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()

RECURSE(
    by_load
)
