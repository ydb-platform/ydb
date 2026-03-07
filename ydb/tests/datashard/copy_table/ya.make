PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_SUBTESTS()
SPLIT_FACTOR(13)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    REQUIREMENTS(cpu:2)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

TEST_SRCS(
    test_copy_table.py
)

PEERDIR(
    ydb/tests/sql/lib
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
