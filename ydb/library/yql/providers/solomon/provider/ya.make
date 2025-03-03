LIBRARY()

SRCS(
    yql_solomon_config.cpp
    yql_solomon_datasink_execution.cpp
    yql_solomon_datasink_type_ann.cpp
    yql_solomon_datasink.cpp
    yql_solomon_datasource_execution.cpp
    yql_solomon_datasource_type_ann.cpp
    yql_solomon_datasource.cpp
    yql_solomon_dq_integration.cpp
    yql_solomon_io_discovery.cpp
    yql_solomon_load_meta.cpp
    yql_solomon_mkql_compiler.cpp
    yql_solomon_physical_optimize.cpp
    yql_solomon_provider.cpp
)

PEERDIR(
    ydb/library/actors/protos
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/opt
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/solomon/actors
    ydb/library/yql/providers/solomon/expr_nodes
    ydb/library/yql/providers/solomon/proto
    ydb/library/yql/providers/solomon/scheme
    ydb/public/sdk/cpp/src/client/types/credentials
    yql/essentials/core/dq_integration
    yql/essentials/providers/common/config
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/transform
    yql/essentials/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()
