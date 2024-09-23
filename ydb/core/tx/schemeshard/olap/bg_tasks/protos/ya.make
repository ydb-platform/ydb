PROTO_LIBRARY()

SRCS(
    data.proto
)

PEERDIR(
    ydb/core/protos
    ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
