PY3TEST()

TEST_SRCS(
    conftest.py
    test_ydb_backup.py
    test_ydb_flame_graph.py
    test_ydb_impex.py
    test_ydb_recursive_remove.py
    test_ydb_scheme.py
    test_ydb_scripting.py
    test_ydb_sql.py
    test_ydb_table.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ENABLE_COLUMN_TABLES="true")

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    contrib/python/pyarrow
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/oss/canonical
    ydb/tests/oss/ydb_sdk_import
)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(30)

END()
