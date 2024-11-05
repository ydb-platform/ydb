PROTO_LIBRARY()

PEERDIR(
    ydb/library/actors/protos
)

SRCS(
    dq_state_load_plan.proto
    dq_tasks.proto
    dq_transport.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
