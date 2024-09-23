LIBRARY()

SRCS(
    synchronization_service.cpp
)

PEERDIR(
    ydb/core/fq/libs/compute/common
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/quota_manager/proto
    ydb/library/actors/core
    ydb/library/actors/protos
    ydb/library/db_pool/protos
    ydb/library/grpc/actor_client
    ydb/library/services
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/lib/operation_id/protos
)

YQL_LAST_ABI_VERSION()

END()
