YQL_LIBRARY()

SRCS(
    rpc_export_secrets.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/core/kqp/common/events
    ydb/core/persqueue/public/schema
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/tx/replication/controller/protos
    ydb/core/tx/scheme_cache
    ydb/services/scheme_secret
)

END()
