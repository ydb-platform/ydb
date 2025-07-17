PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(20)
SIZE(MEDIUM)

TEST_SRCS(
    test_async_replication.py
)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/library
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
