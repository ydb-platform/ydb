LIBRARY(commands)

SRCS(
    ydb_cloud_root.cpp
    ydb_version.cpp
)

IF (DISABLE_UPDATE)
    CFLAGS(
        -DDISABLE_UPDATE
    )
ELSE()
    SRCS(
        ydb_update.cpp
    )
ENDIF ()

PEERDIR(
    ydb/public/sdk/cpp/src/client/iam
    ydb/public/lib/ydb_cli/commands
)

END()
