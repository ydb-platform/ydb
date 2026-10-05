PY3TEST()

TEST_SRCS(
    test_generator.py
    test_init.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:1)
ELSE()
    REQUIREMENTS(ram:16 cpu:2)
ENDIF()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
    contrib/python/PyHamcrest
    ydb/tests/library
)

FORK_SUBTESTS()
FORK_TEST_FILES()
END()
