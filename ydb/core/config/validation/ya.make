LIBRARY()

SRCS(
    validators.h
    validators.cpp
    column_shard_config_validator.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/formats/arrow/serializer
)

END()

RECURSE_FOR_TESTS(
    ut
    column_shard_config_validator_ut
)

