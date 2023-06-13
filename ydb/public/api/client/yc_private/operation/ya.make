PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    operation.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

