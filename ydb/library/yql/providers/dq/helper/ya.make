LIBRARY()

PEERDIR(
    yql/essentials/core/dq_integration
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
)

SRCS(
    yql_dq_helper_impl.cpp
)

END()
