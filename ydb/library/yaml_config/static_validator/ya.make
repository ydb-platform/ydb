LIBRARY()

PEERDIR(
    ydb/library/yaml_config/validator
)

SRCS(
    builders.h
    builders.cpp
)

END()

RECURSE_FOR_TESTS(ut)