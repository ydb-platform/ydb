PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/python
    ydb/public/api/grpc
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
    ydb/apps/ydbd
)

TEST_SRCS(
    test_update_script_tables.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

FORK_SUBTESTS()

END()
