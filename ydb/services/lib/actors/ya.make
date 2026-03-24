LIBRARY()

SRCS(
    consumers_advanced_monitoring_settings.cpp
    pq_schema_actor.cpp
)

PEERDIR(
    ydb/library/grpc/server
    library/cpp/json
    library/cpp/digest/md5
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/metering
    ydb/core/mind
    ydb/core/protos
    ydb/core/util
    ydb/public/sdk/cpp/src/library/persqueue/obfuscate
    ydb/library/persqueue/topic_parser
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/library/jwt
    ydb/public/sdk/cpp/src/library/operation_id
)

END()
