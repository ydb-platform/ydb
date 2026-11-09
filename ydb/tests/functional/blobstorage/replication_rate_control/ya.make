PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_replication_rate_control.py
)

SIZE(MEDIUM)
REQUIREMENTS(ram:20 cpu:2)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

FORK_SUBTESTS()

END()
