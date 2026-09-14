PROTO_LIBRARY(etcd-grpc)
PROTOC_FATAL_WARNINGS()

GRPC()

SRCS(
    auth.proto
    kv.proto
    rpc.proto
)

USE_COMMON_GOOGLE_APIS(api/annotations)

EXCLUDE_TAGS(GO_PROTO)

END()
