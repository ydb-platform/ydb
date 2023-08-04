LIBRARY()

SRCS(
    synchronization_service.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/protos
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/public/api/grpc
    ydb/library/db_pool/protos
    ydb/public/lib/operation_id/protos
    ydb/core/fq/libs/quota_manager/proto
    ydb/public/api/grpc/draft
    ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()
