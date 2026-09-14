PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SET(PROTOC_TRANSITIVE_HEADERS "no")

SRCS(
    events.proto
)

PEERDIR(
    ydb/core/protos
    ydb/public/api/protos
)

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

END()
