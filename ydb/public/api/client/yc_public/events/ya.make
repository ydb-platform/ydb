PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/client/yc_public/common
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    options.proto
    common.proto
    yq.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

