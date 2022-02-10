OWNER(g:kikimr va-kuznecov) 
PY3TEST()
 
TEST_SRCS( 
    test_ydb_backup.py 
    test_ydb_table.py
    test_ydb_scripting.py
) 
 
ENV(YDB_TOKEN="root@builtin")
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
TIMEOUT(600) 
SIZE(MEDIUM) 
 
DEPENDS( 
    ydb/apps/ydbd
    ydb/apps/ydb
) 
 
PEERDIR( 
    ydb/tests/library
) 
 
FORK_SUBTESTS() 
FORK_TEST_FILES() 
 
END() 
