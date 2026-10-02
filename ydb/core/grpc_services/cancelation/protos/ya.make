PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    event.proto
)

PEERDIR(
    ydb/library/actors/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()

