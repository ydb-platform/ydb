PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    sensitive.proto
    validation.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
