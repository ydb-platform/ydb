LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    library/cpp/grpc/server
    ydb/core/grpc_services
    ydb/core/kesus/tablet
    ydb/public/api/grpc
    ydb/services/ydb
)

END()

RECURSE_FOR_TESTS(
    ut
)
