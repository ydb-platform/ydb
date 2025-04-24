LIBRARY()

SRCS(
    etcd_base_init.cpp
    etcd_grpc.cpp
    etcd_impl.cpp
    etcd_shared.cpp
    etcd_watch.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/query
    ydb/apps/etcd_proxy/proto
    ydb/library/grpc/server
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/kesus/tablet
    ydb/core/keyvalue
)

RESOURCE(
    ydb/apps/etcd_proxy/service/create.sql create.sql
)

END()

RECURSE_FOR_TESTS(
    ut
)
