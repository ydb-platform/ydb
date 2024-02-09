PROTO_LIBRARY()

PEERDIR(
    ydb/public/api/client/yc_public/common
    ydb/public/api/client/yc_public/ydb
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    backup.proto
    database.proto
    options.proto
    common.proto
    yq.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

