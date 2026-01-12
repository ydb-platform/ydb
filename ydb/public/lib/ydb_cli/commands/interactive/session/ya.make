LIBRARY()

SRCS(
    session_runner_common.cpp
    sql_session_runner.cpp
)

PEERDIR(
    contrib/libs/ftxui
    library/cpp/colorizer
    library/cpp/yaml/as
    ydb/library/yverify_stream
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/table/query_stats
)

IF (NOT OS_WINDOWS OR USE_SSE4)
    SRCS(
        ai_session_runner.cpp
    )
    PEERDIR(
        ydb/public/lib/ydb_cli/commands/interactive/ai
    )
ENDIF()

END()
