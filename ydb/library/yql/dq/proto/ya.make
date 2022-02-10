PROTO_LIBRARY()

OWNER(
    g:yql g:yql_ydb_core
)

PEERDIR(
    library/cpp/actors/protos
)

SRCS(
    dq_checkpoint.proto
    dq_state_load_plan.proto
    dq_tasks.proto 
    dq_transport.proto 
)

EXCLUDE_TAGS(GO_PROTO)

END()
