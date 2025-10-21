PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    viewer.proto
)

PEERDIR(
    ydb/core/protos
    ydb/core/graph/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
