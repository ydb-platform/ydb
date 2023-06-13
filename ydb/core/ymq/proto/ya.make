PROTO_LIBRARY()

SRCS(
    events.proto
    records.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
