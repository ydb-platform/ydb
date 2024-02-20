LIBRARY()

SRCS(
    console_dumper.cpp
    console_dumper.h
    serialize_deserialize.cpp
    yaml_config.cpp
    yaml_config.h
    yaml_config_helpers.cpp
    yaml_config_helpers.h
    yaml_config_parser.cpp
    yaml_config_parser.h
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/protobuf
    contrib/libs/yaml-cpp
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/cms/console/util
    ydb/core/erasure
    ydb/core/protos
    ydb/core/protos/out
    ydb/library/actors/core
    ydb/library/fyamlcpp
    ydb/library/yaml_config/protos
    ydb/library/yaml_config/public
)

END()

RECURSE(
    deprecated
    protos
    public
    static_validator
    tools
    validator
)

RECURSE_FOR_TESTS(
    ut
    ut_transform
)
