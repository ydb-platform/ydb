PY3TEST()

TEST_SRCS(
    conftest.py
    test_serverless.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
TIMEOUT(600)

REQUIREMENTS(
    cpu:1
    ram:32
)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

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
