PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

TEST_SRCS(
    test_credentials_rotation.py
)

PEERDIR(
    ydb/tests/library/replication_credentials
)

ENV(YDB_USE_IN_MEMORY_PDISKS=false)
FORK_SUBTESTS()
SIZE(LARGE)
TIMEOUT(1800)
REQUIREMENTS(ram:8 cpu:2)

END()
