LIBRARY(cli_base)

SRCS(
    cli_cmds_db.cpp
    cli_cmds_discovery.cpp
    cli_cmds_root.cpp
    cli_kicli.cpp
)

PEERDIR(
    ydb/core/driver_lib/cli_config_base
    ydb/library/aclib
    ydb/public/lib/deprecated/kicli
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/resources
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_types/credentials
    ydb/public/lib/ydb_cli/commands/sdk_core_access
    ydb/public/lib/ydb_cli/commands/ydb_discovery
)

YQL_LAST_ABI_VERSION()

END()
