PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    access_service.proto
    access.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
