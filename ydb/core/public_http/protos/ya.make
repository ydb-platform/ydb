PROTO_LIBRARY()

# gn: move into parent
PROTOC_FATAL_WARNINGS()

SRCS(
    fq.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
