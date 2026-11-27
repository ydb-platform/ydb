YQL_LIBRARY()

SRCS(
    kqp_predictor.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/expr_nodes
    ydb/library/accessor
    ydb/library/actors/core
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/proto
    yql/essentials/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
