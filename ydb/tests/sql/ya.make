PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_kv.py
    test_crud.py
    test_inserts.py
    test_workload_manager.py
)

SIZE(SMALL)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    ydb/tests/sql/lib
)

PEERDIR(
    ydb/tests/library
    ydb/tests/sql/lib
)

END()
