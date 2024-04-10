PROTO_LIBRARY(yaml-config-protos)

SRCS(
    config.proto
)

PEERDIR(
    ydb/core/protos
    ydb/core/config/protos
)

CPP_PROTO_PLUGIN0(config_proto_plugin ydb/core/config/tools/protobuf_plugin)

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

END()
