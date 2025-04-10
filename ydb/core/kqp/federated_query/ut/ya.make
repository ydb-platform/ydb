UNITTEST_FOR(ydb/core/kqp/federated_query)

PEERDIR(
    ydb/core/kqp/federated_query
    ydb/public/api/protos
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/public/issue/protos
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/yt/comp_nodes/dq
    ydb/library/yql/providers/yt/comp_nodes/llvm14
)

SRCS(
    kqp_federated_query_helpers_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
