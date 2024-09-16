PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TEST_SRCS(
    test_schemeshard_limits.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

FORK_TEST_FILES()
FORK_SUBTESTS()

END()
