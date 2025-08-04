PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

EXCLUDE_TAGS(GO_PROTO)

GRPC()

SRCS(
    expression.proto
    validate.proto
    priv/private.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()
