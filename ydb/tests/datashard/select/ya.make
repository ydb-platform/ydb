PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(45)

SIZE(MEDIUM)

REQUIREMENTS(ram:32 cpu:4)

TEST_SRCS(
    test_select.py

)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
