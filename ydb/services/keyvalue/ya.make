LIBRARY()

SRCS(
    grpc_service_v1.cpp
    grpc_service_v2.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/jaeger_tracing
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/public/api/grpc
)

END()

RECURSE_FOR_TESTS(
    ut
)
