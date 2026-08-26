LIBRARY()

SRCS(
    kqp_gateway.cpp
    kqp_ic_gateway.cpp
    kqp_metadata_loader.cpp
)

PEERDIR(
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/kqp/common
    ydb/core/kqp/federated_query
    ydb/core/kqp/federated_query/actors
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/gateway/behaviour/external_data_source
    ydb/services/workload_manager/metadata_subscription
    ydb/services/workload_manager/metadata_subscription/resource_pool_classifier
    ydb/services/workload_manager/service
    ydb/core/kqp/gateway/behaviour/streaming_query
    ydb/core/kqp/gateway/behaviour/table
    ydb/core/kqp/gateway/behaviour/tablestore
    ydb/core/kqp/gateway/behaviour/view
    ydb/core/kqp/gateway/utils
    ydb/core/kqp/provider
    ydb/core/kqp/query_data
    ydb/core/persqueue/public/schema
    ydb/core/statistics/service
    ydb/core/sys_view/common
    ydb/library/actors/core
    yql/essentials/providers/result/expr_nodes
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
