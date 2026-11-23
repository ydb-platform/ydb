PY3TEST()

SIZE(LARGE)

TAG(ya:manual)

TIMEOUT(14400)

TEST_SRCS(
    test_tpcc.py
)

ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")

ENV(YDB_ENABLE_COLUMN_TABLES=true)

ENV(YDB_TEST_DISABLE_MLOCK=1)

ENV(NO_KUBER_LOGS=yes)

ENV(WAIT_CLUSTER_ALIVE_TIMEOUT=600)

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/functional/lbr/lib
    ydb/tests/functional/tpc/lib
    ydb/tests/olap/load/lib
)

END()

RECURSE(
    lib
    ut
)
