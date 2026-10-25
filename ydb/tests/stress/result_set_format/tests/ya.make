PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_WORKLOAD_PATH="ydb/tests/stress/result_set_format/result_set_format")

TEST_SRCS(
    test_arrow_workload.py
    test_value_workload.py
)

IF (NOT SANITIZER_TYPE AND BUILD_TYPE != "DEBUG")
    REQUIREMENTS(ram:32 cpu:2)
ELSE()
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

SIZE(MEDIUM)

DEPENDS(
    ydb/tests/stress/result_set_format
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/result_set_format/workload
    ydb/tests/stress/common
)

END()
