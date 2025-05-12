LIBRARY(tpcc)

SRCS(
    command_root.cpp
    command_run.cpp
    common_queries.cpp
    runner.cpp
    terminal.cpp
    transaction_neworder.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/commands/command_base
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/query
    library/cpp/histogram/hdr
)

END()

RECURSE_FOR_TESTS(
    ut
)
