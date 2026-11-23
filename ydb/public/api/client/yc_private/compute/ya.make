PROTO_LIBRARY()
PROTOC_FATAL_WARNINGS()

SRCS(
    export.proto
)

END()

RECURSE(
    inner
)
