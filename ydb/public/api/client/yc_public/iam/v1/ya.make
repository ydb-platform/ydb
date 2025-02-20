PROTO_LIBRARY()

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    iam_token_service.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

