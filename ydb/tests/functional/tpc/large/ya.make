PY3TEST()

TEST_SRCS(
    test_tpcds.py
    test_tpch_spilling.py
)

SIZE(LARGE)
TAG(ya:fat)


REQUIREMENTS(ram:16)

ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(NO_KUBER_LOGS="yes")

PEERDIR(
    ydb/tests/functional/tpc/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

FORK_TEST_FILES()

END()
