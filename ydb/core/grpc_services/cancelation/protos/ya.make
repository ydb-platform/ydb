PROTO_LIBRARY()

SRCS(
    event.proto
)

PEERDIR(
    ydb/library/actors/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()

