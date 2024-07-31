LIBRARY()

SRCS(
    kqp_gateway.cpp
    kqp_ic_gateway.cpp
    kqp_metadata_loader.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/kqp/provider
    ydb/core/kqp/query_data
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/gateway/behaviour/tablestore
    ydb/core/kqp/gateway/behaviour/table
    ydb/core/kqp/gateway/behaviour/external_data_source
    ydb/core/kqp/gateway/behaviour/resource_pool
    ydb/core/kqp/gateway/behaviour/view
    ydb/core/kqp/gateway/utils
    ydb/core/statistics/service    
    ydb/library/yql/providers/result/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    behaviour
    local_rpc
    utils
)

RECURSE_FOR_TESTS(ut)
