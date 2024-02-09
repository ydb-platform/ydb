UNITTEST_FOR(ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
)

SRCS(
    table_writer_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
