LIBRARY()

SRCS(
    kqp_query_data.cpp
    kqp_prepared_query.cpp
    kqp_predictor.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/common/simple
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/proto
    ydb/core/kqp/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()