LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/library/grpc/server
    ydb/core/grpc_services
    ydb/core/mind
    ydb/public/api/grpc
    ydb/public/lib/operation_id
)

END()
