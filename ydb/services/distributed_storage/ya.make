LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/public/api/grpc/draft
    ydb/library/grpc/server
    ydb/core/grpc_services
    ydb/core/grpc_services/base
)

END()

RECURSE_FOR_TESTS(
    ut
)
