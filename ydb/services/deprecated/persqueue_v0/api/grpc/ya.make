PROTO_LIBRARY(api-grpc-persqueue-deprecated)
PROTOC_FATAL_WARNINGS()

MAVEN_GROUP_ID(com.yandex.ydb)

GRPC()

SRCS(
    persqueue.proto
)

PEERDIR(
    ydb/services/deprecated/persqueue_v0/api/protos
    ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO)

END()
