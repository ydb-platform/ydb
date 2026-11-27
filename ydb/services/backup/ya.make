LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/library/grpc/server
    ydb/public/api/grpc/draft
    ydb/public/sdk/cpp/src/client/resources
)

END()
