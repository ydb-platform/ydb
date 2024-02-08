LIBRARY()

SRCS(
    console_dumper.cpp
    console_dumper.h
    yaml_config.cpp
    yaml_config.h
    yaml_config_parser.cpp
    yaml_config_parser.h
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/protobuf
    contrib/libs/yaml-cpp
    ydb/library/actors/core
    library/cpp/protobuf/json
    ydb/library/fyamlcpp
    ydb/core/base
    ydb/core/cms/console/util
    ydb/core/erasure
    ydb/core/protos
    ydb/library/yaml_config/public
)

END()

RECURSE(
    public
    validator
    static_validator
)

RECURSE_FOR_TESTS(
    ut
)
