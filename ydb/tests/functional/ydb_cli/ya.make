PY3TEST()

TEST_SRCS(
    test_ydb_backup.py
    test_ydb_table.py
    test_ydb_scripting.py
    test_ydb_impex.py
    test_ydb_flame_graph.py
)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")
TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

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

FORK_TEST_FILES()

END()
