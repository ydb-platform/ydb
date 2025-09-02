PROGRAM()

SRCS(
    main.cpp
    commands.cpp
    cmd_drop.cpp
    cmd_prepare.cpp
    cmd_prepare_scheme.cpp
    cmd_run_query.cpp
    cmd_run_bench.cpp
)

PEERDIR(
    ydb/core/kqp/tests/tpch/lib
    library/cpp/json
    ydb/public/lib/ydb_cli/commands
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/yson_value
)

END()

RECURSE(
    lib
)
