UNITTEST_FOR(ydb/core/backup/impl)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/tx/replication/ut_helpers
)

SRCS(
    local_partition_reader_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
