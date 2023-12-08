LIBRARY()

SRCS(
    validator_builder.h
    validator_builder.cpp
    validator.h
    validator.cpp
    validator_checks.h
    validator_checks.cpp
    configurators.h
    configurators.cpp
)

PEERDIR(
    ydb/library/fyamlcpp
)

GENERATE_ENUM_SERIALIZATION(validator_builder.h)

END()

RECURSE_FOR_TESTS(
    ut
)
