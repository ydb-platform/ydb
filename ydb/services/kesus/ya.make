LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_streaming
    ydb/core/kesus/proxy
    ydb/core/kesus/tablet
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/resources
)

END()
