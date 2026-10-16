UNITTEST()

REQUIREMENTS(cpu:1)
SRCS(
    test.cpp
)

PEERDIR(
    ydb/library/yaml_config/static_validator
    ydb/library/yaml_config/validator
)

END()

RECURSE_FOR_TESTS(example_configs)
