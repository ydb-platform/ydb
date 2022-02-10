OWNER(g:kikimr)

PY3TEST()

SPLIT_FACTOR(30)
FORK_SUBTESTS()
FORK_TEST_FILES()
TIMEOUT(600)
SIZE(MEDIUM)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_session_pool.py
    test_crud.py
    test_indexes.py
    test_discovery.py
    test_execute_scheme.py
    test_insert.py
    test_isolation.py
    test_public_api.py
    test_read_table.py
    test_tornado_frameworks.py
    test_s3_listing.py
    test_session_grace_shutdown.py
)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    contrib/python/tornado/tornado-4
    ydb/tests/library
    ydb/public/sdk/python/ydb
)

REQUIREMENTS(ram:10) 
 
END()
