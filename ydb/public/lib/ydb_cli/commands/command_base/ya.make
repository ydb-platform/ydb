LIBRARY(ydb_cli_command_base)

SRCS(
    ../ydb_command.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/driver
)

END()
