LIBRARY()

SRCS(
    kqp_proxy_service.cpp
    kqp_proxy_peer_stats_calculator.cpp
    kqp_script_executions.cpp
    kqp_table_creator.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/http
    library/cpp/protobuf/json
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/cms/console
    ydb/core/kqp/common
    ydb/core/kqp/common/events
    ydb/core/kqp/counters
    ydb/core/kqp/proxy_service/proto
    ydb/core/kqp/run_script_actor
    ydb/core/mind
    ydb/core/protos
    ydb/core/tx/tx_proxy
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/library/query_actor
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/common/proto
    ydb/library/yql/public/issue
    ydb/public/api/protos
    ydb/public/lib/operation_id
    ydb/public/lib/scheme_types
    ydb/public/sdk/cpp/client/ydb_params
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    proto
)

RECURSE_FOR_TESTS(
    ut
)
