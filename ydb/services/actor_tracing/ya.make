LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/library/grpc/server
)

END()

RECURSE_FOR_TESTS(
    ut
)
