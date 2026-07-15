LIBRARY()

SRCS(
    migration/config_migration.cpp
    migration/config_migration.h
    yaml_config.cpp
    yaml_config.h
)

PEERDIR(
    contrib/libs/openssl
    contrib/libs/protobuf
    contrib/libs/yaml-cpp
    ydb/library/actors/core
    library/cpp/protobuf/json
    ydb/library/fyamlcpp
)

END()
