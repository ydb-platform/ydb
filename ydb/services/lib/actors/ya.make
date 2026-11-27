LIBRARY()

SRCS(
    consumers_advanced_monitoring_settings.cpp
    pq_schema_actor.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/persqueue/public
    ydb/core/persqueue/public/schema
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/library/actors/core
    ydb/library/persqueue/topic_parser
    ydb/public/api/grpc/draft
    ydb/public/api/protos
)

END()
