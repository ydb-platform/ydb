LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/core/grpc_services
    ydb/public/api/grpc
    library/cpp/actors/core
    ydb/library/grpc/server
)

END()
