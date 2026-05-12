LIBRARY()

SRCS(
    validators.h
    validators.cpp
    auth_config_validator.cpp
    column_shard_config_validator.cpp
    monitoring_config_validator.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow/serializer
    library/cpp/protobuf/json
)

END()

RECURSE_FOR_TESTS(
    ut
    auth_config_validator_ut
    column_shard_config_validator_ut
)
