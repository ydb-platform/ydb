LIBRARY()

SRCS(
    pq_schema_actor.cpp
    pq_rl_helpers.cpp
)

PEERDIR(
    library/cpp/grpc/server
    library/cpp/digest/md5
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/metering
    ydb/core/mind
    ydb/core/protos
    ydb/library/persqueue/obfuscate
    ydb/library/persqueue/topic_parser
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/lib/jwt
    ydb/public/lib/operation_id
)

END()
