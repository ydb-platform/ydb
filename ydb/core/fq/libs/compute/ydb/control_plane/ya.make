LIBRARY()

SRCS(
    cms_grpc_client_actor.cpp
    compute_database_control_plane_service.cpp
    ydbcp_grpc_client_actor.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/protos
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/quota_manager/proto
    ydb/core/protos
    ydb/library/db_pool/protos
    ydb/library/yql/public/issue
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/lib/operation_id/protos
)

YQL_LAST_ABI_VERSION()

END()
