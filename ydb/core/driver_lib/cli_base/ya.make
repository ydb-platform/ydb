LIBRARY(cli_base)

SRCS(
    cli_cmds_db.cpp
    cli_cmds_discovery.cpp
    cli_cmds_root.cpp
    cli_cmds_whoami.cpp
    cli_kicli.cpp
)

PEERDIR(
    ydb/core/driver_lib/cli_config_base
    ydb/library/aclib
    ydb/public/lib/deprecated/kicli
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/resources
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
