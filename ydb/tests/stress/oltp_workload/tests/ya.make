PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/oltp_workload/workload
    ydb/tests/stress/common
)


END()
