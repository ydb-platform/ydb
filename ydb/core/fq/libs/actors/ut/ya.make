UNITTEST_FOR(ydb/core/fq/libs/actors)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    library/cpp/retry
    library/cpp/testing/unittest
    ydb/core/fq/libs/actors
    ydb/core/mind
    ydb/core/testlib/default
    ydb/library/actors/testlib
)

YQL_LAST_ABI_VERSION()

SRCS(
    database_resolver_ut.cpp
    streaming_query_nodes_manager_ut.cpp
)

END()
