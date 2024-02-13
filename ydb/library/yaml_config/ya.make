LIBRARY()

SRCS(
    console_dumper.cpp
    console_dumper.h
    yaml_config.cpp
    yaml_config.h
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
    ydb/library/actors/core
    ydb/library/fyamlcpp
    ydb/library/yaml_config/public
)

END()

RECURSE(
    deprecated
    public
    static_validator
    validator
)

RECURSE_FOR_TESTS(
    ut
)
