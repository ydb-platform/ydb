PROTO_LIBRARY()

OWNER(
    g:yq
)

SRCS(
    graph_params.proto
)

PEERDIR(
    ydb/library/yql/dq/proto 
    ydb/library/yql/providers/dq/api/protos 
)

EXCLUDE_TAGS(GO_PROTO)

END()
