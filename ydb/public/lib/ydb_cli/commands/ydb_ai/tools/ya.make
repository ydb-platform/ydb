LIBRARY()

SRCS(
    exec_query_tool.cpp
    list_directory_tool.cpp
    tool_interface.cpp
)

PEERDIR(
    library/cpp/json/writer
    ydb/core/base
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/commands/ydb_ai/common
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
)

END()
