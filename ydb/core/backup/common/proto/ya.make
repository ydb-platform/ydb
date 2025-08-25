PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

PEERDIR(
    ydb/public/api/protos/annotations
)

SRCS(
    encrypted_file.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
