LIBRARY()

SRCS(
    yql_ydb_config.cpp
    yql_ydb_dq_integration.cpp
    yql_ydb_load_meta.cpp
    yql_ydb_logical_opt.cpp
    yql_ydb_provider.cpp
    yql_ydb_type_ann.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    ydb/library/yql/providers/ydb/common
    library/cpp/json
    library/cpp/threading/future
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/dq/mkql
    ydb/library/yql/providers/native
    ydb/library/yql/providers/ydb/expr_nodes
    ydb/library/yql/providers/ydb/proto
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/table
    yql/essentials/core
    yql/essentials/core/dq_integration
    yql/essentials/core/sql_types
    yql/essentials/providers/common/dq
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/structured_token
    yql/essentials/providers/common/transform
)

END()

RECURSE_FOR_TESTS(ut)
