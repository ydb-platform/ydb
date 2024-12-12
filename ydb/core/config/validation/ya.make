LIBRARY()

SRCS(
    validators.h
    validators.cpp
    auth_config_validator.cpp
)

PEERDIR(
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    ut
    auth_config_validator_ut
)
