UNITTEST_FOR(ydb/core/kqp/federated_query/actors)

SIZE(LARGE)

TAG(
    ya:fat
)

PEERDIR(
    ydb/core/kqp/federated_query/actors
    ydb/core/kqp/federated_query/actors/ut_service/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

SRCS(
    kqp_federated_query_actors_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
