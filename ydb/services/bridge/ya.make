LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/jaeger_tracing
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/public/api/grpc/draft
)

END()
