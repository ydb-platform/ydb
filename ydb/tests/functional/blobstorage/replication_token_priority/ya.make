PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_replication_token_priority.py
)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
REQUIREMENTS(ram:48 cpu:4)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/clients
)

END()
