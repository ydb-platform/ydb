PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(28)

SIZE(MEDIUM)

TEST_SRCS(
    test_secondary_index.py

)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
