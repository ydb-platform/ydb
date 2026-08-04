FUZZ()

FUZZ_DICTS(
    ydb/core/cms/console/fuzz_targets/admin_config_yaml/admin_config_yaml.dict
)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/monlib/service
    library/cpp/protobuf/json
    ydb/core/cms
    ydb/library/yaml_config
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
