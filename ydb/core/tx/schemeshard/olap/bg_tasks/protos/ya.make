PROTO_LIBRARY()

SRCS(
    data.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
