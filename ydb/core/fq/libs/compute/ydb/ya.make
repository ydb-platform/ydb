LIBRARY()

SRCS(
    actors_factory.cpp
    executer_actor.cpp
    finalizer_actor.cpp
    initializer_actor.cpp
    resources_cleaner_actor.cpp
    result_writer_actor.cpp
    status_tracker_actor.cpp
    stopper_actor.cpp
    ydb_connector_actor.cpp
    ydb_run_actor.cpp
)

PEERDIR(
    ydb/library/actors/protos
    library/cpp/lwtrace/protos
    ydb/core/fq/libs/compute/common
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_storage/proto
    ydb/core/fq/libs/graph_params/proto
    ydb/core/fq/libs/grpc
    ydb/core/fq/libs/quota_manager/proto
    ydb/core/protos
    ydb/core/util
    ydb/library/db_pool/protos
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/minikql/arrow
    ydb/library/yql/providers/dq/api/protos
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/lib/operation_id/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    control_plane
    events
    synchronization_service
)
