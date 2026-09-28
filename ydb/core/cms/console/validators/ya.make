LIBRARY()

SRCS(
    core_validators.cpp
    core_validators.h
    registry.cpp
    registry.h
    validator.cpp
    validator.h
    validator_bootstrap.cpp
    validator_bootstrap.h
    validator_composite_conveyor.cpp
    validator_composite_conveyor.h
    validator_nameservice.cpp
    validator_nameservice.h
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet
    ydb/core/config/validation
    ydb/public/api/protos
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
