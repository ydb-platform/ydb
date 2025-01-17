UNITTEST_FOR(ydb/core/tx/replication/ydb_proxy)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    ydb/public/sdk/cpp/client/ydb_topic
    library/cpp/testing/unittest
)

SRCS(
    partition_end_watcher_ut.cpp
    ydb_proxy_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
