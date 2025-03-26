PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

SIZE(LARGE)
TAG(ya:fat)

TEST_SRCS(
    test_copy_table.py

)

PEERDIR(
    ydb/tests/sql/lib
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

END()
