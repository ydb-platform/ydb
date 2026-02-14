LIBRARY()

SRCS(
    kqp_proxy_service.cpp
    kqp_proxy_databases_cache.cpp
    kqp_proxy_peer_stats_calculator.cpp
    kqp_query_text_cache_service.cpp
    kqp_script_executions.cpp
    kqp_session_info.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/protobuf/json
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/counters
    ydb/core/kqp/gateway/behaviour/resource_pool_classifier
    ydb/core/kqp/gateway/behaviour/streaming_query
    ydb/core/kqp/proxy_service/proto
    ydb/core/kqp/proxy_service/script_executions_utils
    ydb/core/kqp/run_script_actor
    ydb/core/kqp/workload_service
    ydb/core/mind
    ydb/core/mon
    ydb/core/protos
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/library/query_actor
    ydb/library/table_creator
    ydb/library/yql/dq/actors/spilling
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/pq/proto
    ydb/library/yql/providers/s3/actors_factory
    ydb/public/api/protos
    ydb/public/lib/scheme_types
    ydb/public/sdk/cpp/src/client/params
    ydb/public/sdk/cpp/src/library/operation_id
    yql/essentials/providers/common/proto
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    script_executions_utils
)

RECURSE_FOR_TESTS(
    ut
)
