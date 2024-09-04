LIBRARY()

SRCS(
    cms_grpc_client_actor.cpp
    compute_database_control_plane_service.cpp
    compute_databases_cache.cpp
    database_monitoring.cpp
    monitoring_grpc_client_actor.cpp
    monitoring_rest_client_actor.cpp
    ydbcp_grpc_client_actor.cpp
)

PEERDIR(
    library/cpp/json
    ydb/library/actors/core
    ydb/library/actors/protos
    ydb/library/grpc/actor_client
    ydb/core/fq/libs/compute/ydb/synchronization_service
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/quota_manager/proto
    ydb/core/kqp/workload_service/common
    ydb/core/protos
    ydb/library/db_pool/protos
    ydb/library/yql/public/issue
    ydb/library/yql/utils
    ydb/library/yql/utils/actors
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/lib/operation_id/protos
)

YQL_LAST_ABI_VERSION()

END()
