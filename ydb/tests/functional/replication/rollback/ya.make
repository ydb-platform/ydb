PY3TEST()

TEST_SRCS(
    test_destination_rollback.py
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

ENV(YDB_USE_IN_MEMORY_PDISKS=false)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

SIZE(MEDIUM)
REQUIREMENTS(ram:8 cpu:1)
FORK_SUBTESTS()

END()
