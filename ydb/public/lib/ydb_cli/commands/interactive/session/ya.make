LIBRARY()

SRCS(
    ai_session_runner.cpp
    session_runner_common.cpp
    sql_session_runner.cpp
)

PEERDIR(
    library/cpp/colorizer
    ydb/core/base
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/table/query_stats
)

END()
