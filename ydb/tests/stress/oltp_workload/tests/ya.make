PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_workload.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32 cpu:2)
ELSE()
    REQUIREMENTS(ram:32 cpu:1)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/oltp_workload/workload
    ydb/tests/stress/common
)


END()

RECURSE_FOR_TESTS(
    soak
)
