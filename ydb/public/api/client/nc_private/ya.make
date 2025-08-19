PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

EXCLUDE_TAGS(GO_PROTO)

GRPC()

SRCS(
    annotations.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

RECURSE(
    audit
    common
    iam
)
