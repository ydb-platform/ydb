PROTO_LIBRARY(api-protos-persqueue-deprecated)
PROTOC_FATAL_WARNINGS()

MAVEN_GROUP_ID(com.yandex.ydb)

PEERDIR(
    ydb/public/api/protos
)

SRCS(
    persqueue.proto
)

EXCLUDE_TAGS(GO_PROTO)

GENERATE_ENUM_SERIALIZATION(persqueue.pb.h)

END()
