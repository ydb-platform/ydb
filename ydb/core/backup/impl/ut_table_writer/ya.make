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
    table_writer_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
