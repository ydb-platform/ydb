LIBRARY()

SRCS(
    pq_schema_actor.cpp
)

PEERDIR(
    ydb/library/grpc/server
    library/cpp/digest/md5
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/metering
    ydb/core/mind
    ydb/core/protos
    ydb/public/sdk/cpp/src/library/persqueue/obfuscate
    ydb/library/persqueue/topic_parser
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/library/jwt
    ydb/public/sdk/cpp/src/library/operation_id
)

END()
