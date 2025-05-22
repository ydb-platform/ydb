LIBRARY()

SRCS(
    common_queries.cpp
    runner.cpp
    task_queue.cpp
    terminal.cpp
    transaction_neworder.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/query
    library/cpp/histogram/hdr
)

END()

RECURSE_FOR_TESTS(
    ut
)
