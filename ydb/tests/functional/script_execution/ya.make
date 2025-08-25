PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/python
    ydb/public/api/grpc
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
)

TEST_SRCS(
    test_update_script_tables.py
)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

END()
