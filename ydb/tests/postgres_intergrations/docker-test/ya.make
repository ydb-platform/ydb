PY3TEST()

FORK_TEST_FILES()
TIMEOUT(600)
SIZE(LARGE)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
DEPENDS(
    ydb/apps/ydbd
)

ALL_PYTEST_SRCS(

)

PEERDIR(
    contrib/python/requests
    contrib/python/tornado/tornado-4
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

REQUIREMENTS(ram:10)

END()
