PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
PY_SRCS (
    conftest.py
    common.py
)

TEST_SRCS(
    test_rename.py
)


REQUIREMENTS(
    cpu:4
    ram:16
)

FORK_TEST_FILES()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    FORK_SUBTESTS()
    SPLIT_FACTOR(10)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

DEPENDS(ydb/apps/ydbd)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
    contrib/python/tornado/tornado-4
)

END()
