PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_find_tli_chain.py
)

REQUIREMENTS(ram:32 cpu:4)

SIZE(MEDIUM)

PEERDIR(
    ydb/tools/tli_analysis
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/oltp_workload/workload
    ydb/tests/stress/common
    ydb/public/sdk/python
)

END()
