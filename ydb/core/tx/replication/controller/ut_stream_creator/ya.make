UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    library/cpp/testing/unittest
    yql/essentials/sql/v1_dummy
)

SRCS(
    stream_creator_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
