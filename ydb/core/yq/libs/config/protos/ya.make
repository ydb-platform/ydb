OWNER(g:yq)
 
PROTO_LIBRARY() 
 
SRCS( 
    audit.proto 
    checkpoint_coordinator.proto 
    common.proto 
    control_plane_proxy.proto 
    control_plane_storage.proto 
    db_pool.proto 
    gateways.proto 
    issue_id.proto 
    nodes_manager.proto 
    pending_fetcher.proto 
    pinger.proto 
    private_api.proto 
    private_proxy.proto 
    read_actors_factory.proto
    resource_manager.proto 
    storage.proto 
    test_connection.proto 
    token_accessor.proto 
    yq_config.proto 
) 
 
PEERDIR( 
    ydb/library/folder_service/proto
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/s3/proto
) 
 
EXCLUDE_TAGS(GO_PROTO) 
 
END() 
