LIBRARY()

SRCS(
    classic_grpc_service.cpp
    grpc_service.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/libs/service
    ydb/core/nbs/cloud/blockstore/compat/public/api/grpc
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
