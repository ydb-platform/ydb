PROTO_LIBRARY()

PEERDIR(
    ydb/core/config/protos
)

SRCS(
    config_root_test.proto
    copy_to_test.proto
    as_map_test.proto
)

CPP_PROTO_PLUGIN0(config_proto_plugin ydb/core/config/tools/protobuf_plugin)

EXCLUDE_TAGS(GO_PROTO)

END()
