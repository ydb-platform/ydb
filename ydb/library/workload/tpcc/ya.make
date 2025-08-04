LIBRARY()

SRCS(
    check.cpp
    clean.cpp
    common_queries.cpp
    data_splitter.cpp
    histogram.cpp
    init.cpp
    import.cpp
    import_tui.cpp
    log_backend.cpp
    logs_scroller.cpp
    path_checker.cpp
    runner.cpp
    runner_tui.cpp
    scroller.cpp
    task_queue.cpp
    terminal.cpp
    transaction_delivery.cpp
    transaction_neworder.cpp
    transaction_orderstatus.cpp
    transaction_payment.cpp
    transaction_simulation.cpp
    transaction_stocklevel.cpp
    util.cpp
)

PEERDIR(
    contrib/libs/ftxui
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/query
)

GENERATE_ENUM_SERIALIZATION(runner.h)
GENERATE_ENUM_SERIALIZATION_WITH_HEADER(constants.h)

END()

RECURSE_FOR_TESTS(
    ut
)
