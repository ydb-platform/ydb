UNITTEST_FOR(ydb/core/kqp/federated_query/actors)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/mind
    ydb/core/testlib/default
    ydb/library/actors/testlib
    ydb/library/yql/dq/common
    ydb/library/yql/providers/pq/proto
)

YQL_LAST_ABI_VERSION()

SRCS(
    streaming_query_nodes_manager_ut.cpp
)

END()
