PROTO_LIBRARY(etcd-grpc)

GRPC()

SRCS(
    auth.proto
    kv.proto
    rpc.proto
)

USE_COMMON_GOOGLE_APIS(api/annotations)

EXCLUDE_TAGS(GO_PROTO)

END()
