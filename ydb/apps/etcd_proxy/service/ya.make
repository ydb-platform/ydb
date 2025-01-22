LIBRARY()

SRCS(
    etcd_base_init.cpp
    etcd_impl.cpp
    etcd_shared.cpp
    grpc_service.cpp
)

PEERDIR(
    ydb/apps/etcd_proxy/proto
    ydb/library/grpc/server
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/kesus/tablet
    ydb/core/keyvalue
)

RESOURCE(
    ydb/apps/etcd_proxy/service/create.sql create.sql
    ydb/apps/etcd_proxy/service/update.sql update.sql
    ydb/apps/etcd_proxy/service/upsert.sql upsert.sql
)
END()

RECURSE_FOR_TESTS(
    ut
)
