LIBRARY()

SRCS(
    control.cpp
    kqp_batch_operations.cpp
    kqp_event_ids.h
    kqp_event_impl.cpp
    kqp_lwtrace_probes.cpp
    kqp_lwtrace_probes.h
    kqp_resolve.cpp
    kqp_resolve.h
    kqp_ru_calc.cpp
    kqp_script_executions.cpp
    kqp_timeouts.cpp
    kqp_timeouts.h
    kqp_tx_manager.cpp
    kqp_tx.cpp
    kqp_types.cpp
    kqp_types.h
    kqp_user_request_context.cpp
    kqp_user_request_context.h
    kqp_yql.cpp
    kqp_yql.h
    kqp.cpp
    kqp.h
)

PEERDIR(
    ydb/core/base
    ydb/core/engine
    ydb/core/protos
    ydb/core/scheme
    ydb/core/kqp/expr_nodes
    ydb/core/kqp/common/simple
    ydb/core/kqp/common/compilation
    ydb/core/kqp/common/events
    ydb/core/kqp/common/shutdown
    ydb/core/kqp/provider
    ydb/core/tx/long_tx_service/public
    ydb/core/tx/sharding
    ydb/library/yql/dq/expr_nodes
    ydb/library/aclib
    yql/essentials/core/issue
    yql/essentials/core/services
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/common
    yql/essentials/core/dq_integration
    yql/essentials/parser/pg_wrapper/interface
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/library/operation_id/protos
    ydb/core/grpc_services/cancelation
    library/cpp/lwtrace
    #library/cpp/lwtrace/protos
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(kqp_tx_info.h)
GENERATE_ENUM_SERIALIZATION(kqp_yql.h)
GENERATE_ENUM_SERIALIZATION(kqp_resolve.h)

END()

RECURSE(
    compilation
    events
    simple
)
