PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_base.py
    test_sql.py
    test_crud.py
    test_inserts.py
)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
)

END()
