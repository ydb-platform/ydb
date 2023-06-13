LIBRARY()

SRCS(
    dq_function_load_meta.cpp
    dq_function_intent_transformer.cpp
    dq_function_provider.cpp
    dq_function_datasource.cpp
    dq_function_datasink.cpp
    dq_function_type_ann.cpp
    dq_function_physical_optimize.cpp
    dq_function_dq_integration.cpp
)

PEERDIR(
    ydb/library/yql/dq/integration
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/common/dq
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema/mkql
    ydb/library/yql/providers/function/expr_nodes
    ydb/library/yql/providers/function/common
    ydb/library/yql/providers/function/gateway
    ydb/library/yql/providers/function/proto
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/opt
)

YQL_LAST_ABI_VERSION()

END()
