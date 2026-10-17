PROTO_LIBRARY()

# gn: move into parent
PROTOC_FATAL_WARNINGS()

SRCS(
    compile_controller.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
