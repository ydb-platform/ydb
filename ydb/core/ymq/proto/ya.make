PROTO_LIBRARY()

SRCS(
    records.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
