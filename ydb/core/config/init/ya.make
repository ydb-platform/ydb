LIBRARY()

SRCS(
    init.h
    init.cpp
    init_noop.cpp
    dummy.h
    dummy.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/driver_lib/cli_base
    ydb/core/driver_lib/cli_config_base
    ydb/core/protos
    ydb/library/yaml_config
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/public/lib/deprecated/kicli
    ydb/public/sdk/cpp/client/ydb_discovery
    ydb/public/sdk/cpp/client/ydb_driver
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

