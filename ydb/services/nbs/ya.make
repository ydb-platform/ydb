LIBRARY()

SRCS(
    classic_grpc_service.cpp
    classic_grpc_service_adapter.cpp
    grpc_service.cpp
)

PEERDIR(
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/public/api/grpc
    ydb/library/grpc/server
    ydb/core/grpc_services
    ydb/core/grpc_services/base
)

END()

RECURSE_FOR_TESTS(
    ut
)
