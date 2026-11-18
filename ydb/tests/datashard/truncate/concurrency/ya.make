PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_truncate_table_concurrency.py
)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

END()
