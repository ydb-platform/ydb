UNITTEST_FOR(ydb/core/tx/replication/ydb_proxy)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
    ydb/public/sdk/cpp/client/ydb_topic
)

SRCS(
    ydb_proxy_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
