PY3TEST()

TEST_SRCS(
    conftest.py
    test_serverless.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
TIMEOUT(600)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

SIZE(MEDIUM)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    contrib/python/tornado/tornado-4
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

END()
