UNITTEST_FOR(ydb/core/kqp/federated_query)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/kqp/federated_query
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

SRCS(
    kqp_federated_query_actors_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
