PY3TEST()

TEST_SRCS(
    test_ydb_backup.py
    test_ydb_table.py
    test_ydb_scripting.py
    test_ydb_impex.py
)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TIMEOUT(600)
SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
    ydb/apps/ydb
)

PEERDIR(
    contrib/python/pyarrow
    ydb/tests/library
    ydb/tests/oss/canonical
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
