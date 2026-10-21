UNITTEST_FOR(ydb/core/fq/libs/state)

SRCS(
    dq_state_load_plan_ut.cpp
)

PEERDIR(
    ydb/core/kqp/federated_query/actors/pq_checkpoint_provider_integration
    ydb/library/actors/testlib
    ydb/library/testlib/pq_helpers
    ydb/library/yql/providers/dq/api/protos
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
