FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/yaml-cpp
    library/cpp/protobuf/json
    ydb/library/yaml_config
    ydb/library/yaml_json
)

END()
