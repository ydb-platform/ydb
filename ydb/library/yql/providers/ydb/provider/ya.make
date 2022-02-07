LIBRARY()

OWNER(g:yql)

SRCS(
    yql_ydb_datasink.cpp
    yql_ydb_datasink_execution.cpp
    yql_ydb_datasink_type_ann.cpp
    yql_ydb_datasource.cpp
    yql_ydb_datasource_type_ann.cpp
    yql_ydb_dq_integration.cpp
    yql_ydb_exec.cpp
    yql_ydb_io_discovery.cpp
    yql_ydb_load_meta.cpp
    yql_ydb_logical_opt.cpp
    yql_ydb_physical_opt.cpp
    yql_ydb_mkql_compiler.cpp
    yql_ydb_provider.cpp
    yql_ydb_provider_impl.cpp
    yql_ydb_settings.cpp
)

PEERDIR(
    library/cpp/random_provider
    library/cpp/time_provider
    library/cpp/yson/node
    ydb/core/yq/libs/common
    ydb/core/yq/libs/db_resolver
    ydb/library/yql/ast
    ydb/library/yql/minikql
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/structured_token
    ydb/library/yql/providers/common/token_accessor/client
    ydb/public/lib/experimental
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_table
    ydb/library/yql/core
    ydb/library/yql/core/type_ann
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/providers/common/config
    ydb/library/yql/providers/common/dq
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/schema/expr
    ydb/library/yql/providers/common/transform
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/dq/interface
    ydb/library/yql/providers/result/expr_nodes
    ydb/library/yql/providers/ydb/expr_nodes
    ydb/library/yql/providers/ydb/proto
)

YQL_LAST_ABI_VERSION()

END()
