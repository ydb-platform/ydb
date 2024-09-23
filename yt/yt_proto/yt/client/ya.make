PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

PY_NAMESPACE(yt_proto.yt.client)

PEERDIR(
    yt/yt_proto/yt/core
)

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

SRCS(
    api/rpc_proxy/proto/api_service.proto
    api/rpc_proxy/proto/discovery_service.proto

    bundle_controller/proto/bundle_controller_service.proto

    cell_master/proto/cell_directory.proto

    chaos_client/proto/replication_card.proto

    chunk_client/proto/data_statistics.proto
    chunk_client/proto/chunk_meta.proto
    chunk_client/proto/read_limit.proto
    chunk_client/proto/chunk_spec.proto
    chunk_client/proto/confirm_chunk_replica_info.proto

    discovery_client/proto/discovery_client_service.proto

    hive/proto/timestamp_map.proto
    hive/proto/cluster_directory.proto

    node_tracker_client/proto/node.proto
    node_tracker_client/proto/node_directory.proto

    table_chunk_format/proto/chunk_meta.proto
    table_chunk_format/proto/column_meta.proto
    table_chunk_format/proto/wire_protocol.proto

    tablet_client/proto/lock_mask.proto

    transaction_client/proto/timestamp_service.proto

    query_client/proto/query_statistics.proto

    misc/proto/workload.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
