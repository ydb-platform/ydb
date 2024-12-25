PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

REQUIREMENTS(ram:32)
TIMEOUT(1800)

TEST_SRCS(
    test_inserts_tpch.py
)

SIZE(LARGE)
TAG(ya:fat)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    ydb/tests/sql/lib
)

PEERDIR(
    ydb/tests/library
    ydb/tests/sql/lib
)

FORK_SUBTESTS()

END()
