PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/client/yc_public/events
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    yq.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

