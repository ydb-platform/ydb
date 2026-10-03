LIBRARY(commands)

SRCS(
    ydb_echo.cpp
    ydb_root.cpp
    ydb_update.cpp
    ydb_version.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/lib/ydb_cli/commands
)

END()
