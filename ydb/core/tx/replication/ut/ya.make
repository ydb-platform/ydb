UNITTEST_FOR(ydb/core/tx/replication)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/core/persqueue/ut/common
    ydb/core/testlib/default
    ydb/core/tx/replication/ut_helpers
    ydb/core/tx/schemeshard/ut_helpers

    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils
    ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils

    library/cpp/testing/unittest
)

SRCS(
    replication_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
