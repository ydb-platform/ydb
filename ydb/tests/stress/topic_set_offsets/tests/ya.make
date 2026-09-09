PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_TEST_PATH="ydb/tests/stress/topic_set_offsets/topic_set_offsets")

TEST_SRCS(
    test_workload.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    ENV(YDB_STRESS_TEST_LIMIT_MEMORY=1)
ENDIF()

DEPENDS(
    ydb/apps/ydb
    ydb/tests/stress/topic_set_offsets
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/topic_set_offsets/workload
)

END()
