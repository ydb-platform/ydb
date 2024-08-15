LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/library/grpc/server
    ydb/core/grpc_services
    ydb/core/protos
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/library/operation_id
)

END()
