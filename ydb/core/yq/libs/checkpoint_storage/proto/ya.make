PROTO_LIBRARY()

PEERDIR(
    ydb/core/yq/libs/graph_params/proto
)

SRCS(
    graph_description.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
