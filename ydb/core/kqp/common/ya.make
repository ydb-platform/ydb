LIBRARY()

SRCS(
    kqp_event_ids.h
    kqp_event_impl.cpp
    kqp_resolve.cpp
    kqp_resolve.h
    kqp_ru_calc.cpp
    kqp_yql.cpp
    kqp_yql.h
    kqp_timeouts.h
    kqp_timeouts.cpp
    kqp_lwtrace_probes.h
    kqp_lwtrace_probes.cpp
    kqp_types.h
    kqp_types.cpp
    kqp.cpp
    kqp.h
)

PEERDIR(
    ydb/core/base
    ydb/core/engine
    ydb/core/kqp/expr_nodes
    ydb/core/kqp/provider
    ydb/core/tx/long_tx_service/public
    ydb/core/tx/sharding
    ydb/library/yql/dq/expr_nodes
    ydb/library/aclib
    ydb/library/yql/core/issue
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/common
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/core/grpc_services/cancelation
    library/cpp/lwtrace
    #library/cpp/lwtrace/protos
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(kqp_tx_info.h)
GENERATE_ENUM_SERIALIZATION(kqp_yql.h)

END()
