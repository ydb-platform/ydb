LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/public/api/grpc
)

END()
