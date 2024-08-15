LIBRARY()

SRCS(
    util.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/types/status
)

END()
