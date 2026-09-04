PROTO_LIBRARY()

GRPC()

EXCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    service.proto
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/public/api/protos
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

GO_GRPC_GATEWAY_SRCS(
    service.proto
)

END()
