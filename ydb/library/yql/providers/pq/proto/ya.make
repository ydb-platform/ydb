PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    dq_io.proto
    dq_io_state.proto
    dq_pq_control_plane.proto
    dq_task_params.proto
)

PEERDIR(
    ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
