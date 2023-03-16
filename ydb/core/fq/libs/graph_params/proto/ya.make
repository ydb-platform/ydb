PROTO_LIBRARY()

SRCS(
    graph_params.proto
)

PEERDIR(
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/dq/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
