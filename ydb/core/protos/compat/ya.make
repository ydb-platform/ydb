PROTO_LIBRARY()

IF (OS_WINDOWS)
    NO_OPTIMIZE_PY_PROTOS()
ENDIF()

RUN_PROGRAM(
    ydb/core/protos/compat/codegen
        ../config.proto
        config_compat.proto
    IN ../config.proto
    OUT config_compat.proto
    OUTPUT_INCLUDES
        ydb/library/yaml_config/protos/config.proto
)

PEERDIR(
    ydb/core/protos
    ydb/library/yaml_config/protos
)

CPP_PROTO_PLUGIN0(config_proto_plugin ydb/core/config/tools/protobuf_plugin)

EXCLUDE_TAGS(GO_PROTO JAVA_PROTO)

END()

RECURSE(
    codegen
)
