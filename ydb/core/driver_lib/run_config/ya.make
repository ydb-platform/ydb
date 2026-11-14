LIBRARY()

SRCS(
    config.cpp
    config.h
    resource_broker_config.cpp
    resource_broker_config.h
)

PEERDIR(
    ydb/core/base
    ydb/core/config/init
    ydb/core/driver_lib/cli_config_base
    ydb/core/memory_controller
    ydb/core/protos
    ydb/library/global_plugins
)

YQL_LAST_ABI_VERSION()

END()
