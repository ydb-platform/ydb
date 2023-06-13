LIBRARY()

SRCS(
    console_dumper.cpp
    console_dumper.h
    yaml_config.cpp
    yaml_config.h
    yaml_config_impl.h
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/actors/core
    library/cpp/protobuf/json
    library/cpp/yaml/fyamlcpp
    ydb/core/cms/console/util
    ydb/core/protos
    ydb/library/yaml_config
)

END()

RECURSE_FOR_TESTS(
    ut
)
