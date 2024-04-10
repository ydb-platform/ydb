PROGRAM(yaml-to-proto-dump)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/yaml_config
    ydb/library/yaml_config/deprecated
    ydb/library/yaml_config/tools/util
)

END()
