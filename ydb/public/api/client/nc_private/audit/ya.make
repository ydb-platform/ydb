PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()

SRCS(
    annotations.proto
)

PEERDIR(
    ydb/public/api/client/nc_private/audit/v1/common
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

RECURSE(
    v1
)
