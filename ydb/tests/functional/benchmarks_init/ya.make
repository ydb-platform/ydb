PY3TEST()

TEST_SRCS(
    test_generator.py
    test_init.py
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(ram:8)
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
