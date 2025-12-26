LIBRARY()

SRCS(
    describe_tool.cpp
    exec_query_tool.cpp
    list_directory_tool.cpp
    tool_base.cpp
    tool_interface.cpp
    ydb_help_tool.cpp
    exec_shell_tool.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/json/writer
    ydb/library/yverify_stream
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/commands/interactive/highlight
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
)

END()
