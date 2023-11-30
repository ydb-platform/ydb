LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/public/api/grpc
    ydb/library/actors/core
    ydb/library/grpc/server
)

END()
