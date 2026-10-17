LIBRARY()

SRCS(
    check.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    clean.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    common_queries.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    data_splitter.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    histogram.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    init.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    import.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    import_tui.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    log_backend.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    logs_scroller.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    path_checker.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    runner.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    runner_tui.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    scroller.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    task_queue.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    terminal.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transaction_delivery.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transaction_neworder.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transaction_orderstatus.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transaction_payment.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transaction_simulation.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    transaction_stocklevel.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    tui_base.cpp  # gn: into ydb/public/lib/ydb_cli/commands
    util.cpp  # gn: into ydb/public/lib/ydb_cli/commands
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
