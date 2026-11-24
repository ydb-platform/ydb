PY3TEST()
FORK_SUBTESTS()
SPLIT_FACTOR(5)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

TEST_SRCS(
    test_quota_exhaustion.py
)

REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
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
