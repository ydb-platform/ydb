PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_feature_index_soak.py
)

REQUIREMENTS(ram:32 cpu:4)
TAG(ya:external)

SIZE(LARGE)
TAG(ya:fat)
TIMEOUT(3600)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/stress/oltp_workload/workload
    ydb/tests/stress/common
)

END()
