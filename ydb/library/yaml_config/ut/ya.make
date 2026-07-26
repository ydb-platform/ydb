UNITTEST_FOR(ydb/library/yaml_config)

PEERDIR(
    ydb/library/yaml_config/ut/protos
)

SRCS(
    console_dumper_ut.cpp
    yaml_config_helpers_ut.cpp
    yaml_config_ut.cpp
    yaml_config_parser_ut.cpp
    yaml_config_proto2yaml_ut.cpp
    incompatibility_rules_ut.cpp
    reserved_fields_ut.proto
    reserved_fields_ut.cpp
)

END()
