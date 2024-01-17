LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/library/grpc/server
    library/cpp/lwtrace
    ydb/core/grpc_services
    ydb/core/protos
    ydb/library/login
    ydb/public/api/grpc
    ydb/public/lib/operation_id
)

END()
