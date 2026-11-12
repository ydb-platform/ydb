PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(8)
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
REQUIREMENTS(cpu:4 ram:16)

TEST_SRCS(
    test_snapshot_retention.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/snapshot_retention
    ydb/tests/stress/snapshot_retention/workload
)

END()
