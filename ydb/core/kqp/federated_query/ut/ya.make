UNITTEST_FOR(ydb/core/kqp/federated_query)

PEERDIR(
    ydb/core/kqp/federated_query
    ydb/public/api/protos
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/issue/protos
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
)

SRCS(
    kqp_federated_query_helpers_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
