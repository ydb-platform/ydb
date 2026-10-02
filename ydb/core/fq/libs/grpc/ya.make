PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

GRPC()

SRCS(
    fq_private_v1.proto
)

PEERDIR(
    ydb/public/api/protos
    ydb/core/fq/libs/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
