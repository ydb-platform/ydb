LIBRARY()

SRCS(
    etcd_impl.cpp
    grpc_service.cpp
)

PEERDIR(
    ydb/public/api/grpc/etcd
    ydb/library/grpc/server
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/kesus/tablet
    ydb/core/keyvalue
)

END()

RECURSE_FOR_TESTS(
    ut
)
