PROTO_LIBRARY(api-grpc-persqueue-deprecated)

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
