PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/stress/reconfig_state_storage_workload/tests/common.inc)

TEST_SRCS(
    test_all_workload.py
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()

RECURSE_FOR_TESTS(
    large
)
