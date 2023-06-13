LIBRARY()

SRCS(
    local_pgwire_connection.cpp
    local_pgwire.cpp
    local_pgwire.h
    local_pgwire_util.h
    log_impl.h
    pgwire_kqp_proxy.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/library/yql/public/udf
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/executer_actor
    ydb/core/grpc_services
    ydb/core/grpc_services/local_rpc
    ydb/core/protos
    ydb/core/pgproxy
    ydb/core/ydb_convert
    ydb/public/api/grpc
    ydb/public/lib/operation_id/protos
)

YQL_LAST_ABI_VERSION()

END()
