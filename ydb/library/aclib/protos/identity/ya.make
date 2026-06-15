PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    user_token.proto
)

PEERDIR(
    ydb/public/api/protos/annotations
)

EXCLUDE_TAGS(GO_PROTO)

END()
