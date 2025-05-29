PY3TEST()
ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")

TEST_SRCS(
    test_clean.py
    test_clickbench.py
#    test_external.py
    test_diff_processing.py
    test_tpch.py
)

SIZE(MEDIUM)

REQUIREMENTS(ram:16)

ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(NO_KUBER_LOGS="yes")
ENV(WAIT_CLUSTER_ALIVE_TIMEOUT="60")
ENV(ARCADIA_EXTERNAL_DATA=ydb/tests/functional/tpc/data)

PEERDIR(
    ydb/tests/functional/tpc/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

DATA(
    arcadia/ydb/tests/functional/clickbench/data/hits.csv
    arcadia/ydb/tests/functional/tpc/data
)

FORK_TEST_FILES()

END()
