PY3TEST()
SRCDIR(ydb/tests/stress/reconfig_state_storage_workload/tests)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/stress/reconfig_state_storage_workload/tests/common.inc)

TEST_SRCS(
    test_board_workload.py
    test_scheme_board_workload.py
    test_state_storage_workload.py
)

FORK_TEST_FILES()
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

END()
