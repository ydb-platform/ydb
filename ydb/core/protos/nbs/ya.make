PROTO_LIBRARY()

SET(PROTOC_TRANSITIVE_HEADERS "no")

GRPC()

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

SRCS(
    blockstore.proto
)

CPP_PROTO_PLUGIN0(config_proto_plugin ydb/core/config/tools/protobuf_plugin)

EXCLUDE_TAGS(GO_PROTO)

END()
