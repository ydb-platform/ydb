PY3TEST()

TEST_SRCS(
    test_generator.py
    test_init.py
)

SIZE(MEDIUM)

IF(NOT SANITIZER_TYPE)
    REQUIREMENTS(ram:8)
ELSE()
    REQUIREMENTS(ram:16)
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
