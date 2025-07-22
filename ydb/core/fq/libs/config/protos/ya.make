PROTO_LIBRARY()

SRCS(
    activation.proto
    audit.proto
    checkpoint_coordinator.proto
    common.proto
    compute.proto
    control_plane_proxy.proto
    control_plane_storage.proto
    db_pool.proto
    fq_config.proto
    gateways.proto
    health_config.proto
    issue_id.proto
    nodes_manager.proto
    pending_fetcher.proto
    pinger.proto
    private_api.proto
    private_proxy.proto
    quotas_manager.proto
    rate_limiter.proto
    read_actors_factory.proto
    resource_manager.proto
    row_dispatcher.proto
    storage.proto
    task_controller.proto
    test_connection.proto
    token_accessor.proto
)

PEERDIR(
    ydb/library/folder_service/proto
    ydb/library/yql/dq/actors/protos
    yql/essentials/providers/common/proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
