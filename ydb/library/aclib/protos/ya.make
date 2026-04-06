PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

INCLUDE_TAGS(GO_PROTO)

SRCS(
    aclib.proto
)

PEERDIR(
    ydb/public/api/protos/annotations
)

END()
