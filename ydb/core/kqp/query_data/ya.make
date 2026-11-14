YQL_LIBRARY()

SRCS(
    kqp_query_data.cpp
    kqp_prepared_query.cpp
    kqp_request_predictor.cpp
    kqp_type_mismatch.cpp
)

PEERDIR(
    library/cpp/protobuf/util
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/common/result_set_format
    ydb/core/kqp/common/simple
    ydb/core/kqp/expr_nodes
    ydb/core/kqp/predictor
    ydb/library/actors/core
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/proto
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/library/operation_id/protos
    yql/essentials/providers/result/expr_nodes
)

END()
