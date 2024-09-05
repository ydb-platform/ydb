LIBRARY()

SRCS(
    local_pgwire_auth_actor.cpp
    local_pgwire_connection.cpp
    local_pgwire.cpp
    local_pgwire.h
    local_pgwire_util.cpp
    local_pgwire_util.h
    log_impl.h
    pgwire_kqp_proxy.cpp
    sql_parser.cpp
    sql_parser.h
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/kqp/executer_actor
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/local_rpc
    ydb/core/protos
    ydb/core/pgproxy
    ydb/core/ydb_convert
    ydb/public/api/grpc
    ydb/public/lib/operation_id/protos
    ydb/services/persqueue_v1/actors
)

YQL_LAST_ABI_VERSION()

END()
