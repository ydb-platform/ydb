YQL_LIBRARY()

SRCS(
    rpc_export_secret.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/kqp/common/events
    ydb/core/persqueue/public/schema
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/tx/scheme_cache
    ydb/services/scheme_secret
)

END()

RECURSE_FOR_TESTS(
    ut
)
