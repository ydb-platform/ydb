UNITTEST_FOR(ydb/core/graph/shard)

SIZE(SMALL)

SRC(
    shard_ut.cpp
)

PEERDIR(
    ydb/library/actors/helpers
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/testlib/default
    yql/essentials/sql/v1_dummy
)

YQL_LAST_ABI_VERSION()

END()
