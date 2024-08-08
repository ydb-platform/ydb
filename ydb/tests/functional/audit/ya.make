PY3TEST()

FORK_SUBTESTS()
FORK_TEST_FILES()
TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
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
