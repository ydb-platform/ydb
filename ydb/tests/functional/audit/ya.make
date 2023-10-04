PY3TEST()

SPLIT_FACTOR(30)
FORK_SUBTESTS()
FORK_TEST_FILES()
TIMEOUT(600)
SIZE(MEDIUM)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    conftest.py
    test_auditlog.py
)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

REQUIREMENTS(ram:10)

END()
