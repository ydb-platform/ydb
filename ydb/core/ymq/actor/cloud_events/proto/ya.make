PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

PEERDIR(
    ydb/public/api/client/yc_public/events
    ydb/public/api/client/yc_public/common
)

GRPC()
SRCS(
    ymq.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
