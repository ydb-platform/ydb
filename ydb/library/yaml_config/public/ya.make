LIBRARY()

SRCS(
    yaml_config.cpp
    yaml_config.h
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/protobuf
    contrib/libs/yaml-cpp
    library/cpp/actors/core
    library/cpp/protobuf/json
    library/cpp/yaml/fyamlcpp
)

END()
