LIBRARY()

SRCS(
    grpc_service.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/core/grpc_services
    ydb/core/test_tablet
)

END()

RECURSE_FOR_TESTS(
    ut
)
