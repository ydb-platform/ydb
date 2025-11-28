PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(STRESS_TEST_UTILITY="ydb/tests/stress/scheme_board/pile_promotion/pile_promotion_workload")

TEST_SRCS(
    pile_promotion_test.py
    test_scheme_board_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/scheme_board/pile_promotion
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
    ydb/tests/library/stress
    ydb/tests/stress/common
    ydb/tests/stress/scheme_board/pile_promotion/workload
)


END()
