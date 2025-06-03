LIBRARY()

SRCS(
    common_queries.cpp
    histogram.cpp
    runner.cpp
    task_queue.cpp
    terminal.cpp
    transaction_delivery.cpp
    transaction_neworder.cpp
    transaction_orderstatus.cpp
    transaction_payment.cpp
    transaction_simulation.cpp
    transaction_stocklevel.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/query
)

END()

RECURSE_FOR_TESTS(
    ut
)
