UNITTEST_FOR(ydb/core/tx/replication/controller)

FORK_SUBTESTS()

SIZE(MEDIUM)
TIMEOUT(600)

PEERDIR(
    ydb/core/tx/replication/ut_helpers
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/query
    yql/essentials/udfs/common/json2
    library/cpp/testing/unittest
)

SRCS(
    fulltext_replication_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
