PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_set_not_null.py
)

FORK_SUBTESTS()
FORK_TEST_FILES()

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
