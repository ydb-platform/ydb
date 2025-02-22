LIBRARY(commands)

SRCS(
    ydb_root.cpp
    ydb_update.cpp
    ydb_version.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/lib/ydb_cli/commands
)

END()
