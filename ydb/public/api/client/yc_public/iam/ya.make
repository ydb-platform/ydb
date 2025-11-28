PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    ydb/public/api/client/yc_public/common
)

EXCLUDE_TAGS(GO_PROTO)

GRPC()
SRCS(
    iam_token_service.proto
)

USE_COMMON_GOOGLE_APIS(
    api/annotations
)

END()

