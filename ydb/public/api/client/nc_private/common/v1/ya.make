PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()

SRCS(
    metadata.proto
    operation.proto
)

PEERDIR(
    ydb/public/api/client/nc_private
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
