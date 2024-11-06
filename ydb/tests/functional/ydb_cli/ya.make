PY3TEST()

TEST_SRCS(
    conftest.py
    test_ydb_backup.py
    test_ydb_table.py
    test_ydb_scripting.py
    test_ydb_impex.py
    test_ydb_flame_graph.py
    test_ydb_scheme.py
    test_ydb_sql.py
)

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

IF (SANITIZER_TYPE)
    TIMEOUT(2400)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

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
