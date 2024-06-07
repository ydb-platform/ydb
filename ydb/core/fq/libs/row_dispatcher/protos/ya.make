PROTO_LIBRARY()

SRCS(
    events.proto
)

PEERDIR(
    ydb/library/actors/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
