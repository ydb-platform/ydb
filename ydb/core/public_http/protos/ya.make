PROTO_LIBRARY()

SRCS(
    fq.proto
)

PEERDIR(
    ydb/core/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
