UNITTEST_FOR(ydb/core/tx/replication/ydb_proxy)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    ydb/public/sdk/cpp/client/ydb_topic
    library/cpp/testing/unittest
)

SRCS(
    ydb_proxy_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
