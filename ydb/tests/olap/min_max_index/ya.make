PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_minmax_index.py
)

SIZE(MEDIUM)

PEERDIR(
    ydb/tests/sql/lib
    ydb/tests/olap/scenario/helpers
)

DEPENDS(
    ydb/apps/ydb
)

END()
