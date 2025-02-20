PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    access_service.proto
    resource.proto
    sensitive.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
