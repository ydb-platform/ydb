LIBRARY()

SRCS(
    yaml_config.h
    yaml_config_impl.h
    yaml_config.cpp
)

PEERDIR(
    library/cpp/yaml/fyamlcpp
    contrib/libs/openssl
    ydb/core/protos
    library/cpp/actors/core
    library/cpp/protobuf/json
)

END()

RECURSE_FOR_TESTS(
    ut
)
