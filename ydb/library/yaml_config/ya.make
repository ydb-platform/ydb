LIBRARY()

SRC(yaml_config_parser.cpp)

PEERDIR(
    contrib/libs/protobuf
    contrib/libs/yaml-cpp
    ydb/core/base
    ydb/core/erasure
    ydb/core/protos
)

END()
