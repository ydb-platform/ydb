UNITTEST_FOR(ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    ydb/core/tx/replication/ydb_proxy
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/unittest
)

SRCS(
    topic_reader_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
