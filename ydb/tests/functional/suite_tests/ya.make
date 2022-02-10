OWNER(g:kikimr) 
 
PY3TEST() 
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(USE_IN_MEMORY_PDISKS=true) 
TEST_SRCS( 
    test_base.py 
    test_postgres.py 
    test_sql_logic.py 
    test_stream_query.py 
) 
 
TIMEOUT(600) 
SIZE(MEDIUM) 
 
DEPENDS( 
    ydb/apps/ydbd
) 
 
DATA ( 
    arcadia/ydb/tests/functional/suite_tests/postgres 
    arcadia/ydb/tests/functional/suite_tests/sqllogictest 
) 
 
PEERDIR( 
    ydb/tests/library 
) 
 
FORK_SUBTESTS() 
FORK_TEST_FILES() 
 
REQUIREMENTS(ram:12)

END() 
