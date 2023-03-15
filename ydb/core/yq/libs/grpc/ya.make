PROTO_LIBRARY()

GRPC()

SRCS(
    fq_private_v1.proto
)

PEERDIR(
    ydb/public/api/protos
    ydb/core/yq/libs/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
