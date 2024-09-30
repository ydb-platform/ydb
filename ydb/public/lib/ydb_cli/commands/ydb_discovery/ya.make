LIBRARY(ydb_cli_command_ydb_discovery)

SRCS(
    ../ydb_service_discovery.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/commands/command_base
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/ydb_discovery
)

END()
