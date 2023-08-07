PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TEST_SRCS(
    test_schemeshard_limits.py
)

REQUIREMENTS(
    ram:16
)

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
