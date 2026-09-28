UNITTEST_FOR(ydb/core/kqp/federated_query/actors/pq_checkpoint_provider_integration)

SRCS(
    pq_checkpoint_provider_integration_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/library/actors/testlib
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
