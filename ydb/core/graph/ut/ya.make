UNITTEST_FOR(ydb/core/graph)

SIZE(SMALL)

SRC(
    graph_ut.cpp
)

PEERDIR(
    ydb/library/actors/helpers
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/testlib/default
    ydb/core/graph/shard
    ydb/core/graph/service
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

END()
