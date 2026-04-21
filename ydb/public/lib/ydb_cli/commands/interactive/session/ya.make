LIBRARY()

SRCS(
    ai_session_runner.cpp
    session_runner_common.cpp
    sql_session_runner.cpp
)

PEERDIR(
    contrib/libs/ftxui
    library/cpp/colorizer
    library/cpp/yaml/as
    ydb/library/yverify_stream
    ydb/public/lib/ydb_cli/commands/interactive/ai
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/table/query_stats
)

END()
