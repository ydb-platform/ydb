PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

PY_SRCS(
    placement.py
)

TEST_SRCS(
    test_credentials_rotation.py
)

DEPENDS(
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    ydb/tests/library/compatibility
    ydb/tests/library/replication_credentials
)

ENV(YDB_USE_IN_MEMORY_PDISKS=false)
FORK_SUBTESTS()
SIZE(LARGE)
TIMEOUT(2400)
REQUIREMENTS(ram:8 cpu:2)

END()
