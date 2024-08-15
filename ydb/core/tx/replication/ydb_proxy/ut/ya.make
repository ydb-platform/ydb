UNITTEST_FOR(ydb/core/tx/replication/ydb_proxy)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    ydb/public/sdk/cpp/src/client/topic
    library/cpp/testing/unittest
)

SRCS(
    ydb_proxy_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
