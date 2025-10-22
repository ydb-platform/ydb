PY3TEST()
FORK_SUBTESTS()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

TEST_SRCS(
    test_quota_exhaustion.py
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
ydb/tests/library
ydb/tests/library/test_meta
)

END()

