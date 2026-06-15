LIBRARY()

SRCS(
    describe_tool.cpp
    docs_search_tool.cpp
    exec_query_tool.cpp
    exec_shell_tool.cpp
    explain_query_tool.cpp
    list_directory_tool.cpp
    tool_base.cpp
    tool_interface.cpp
    ydb_help_tool.cpp
)

PEERDIR(
    contrib/libs/yaml-cpp
    library/cpp/archive
    library/cpp/colorizer
    library/cpp/json/writer
    library/cpp/resource
    library/cpp/yaml/as
    ydb/library/yverify_stream
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/commands/interactive/common
    ydb/public/lib/ydb_cli/commands/interactive/highlight
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
)

IF(YDB_CLI_AI_INCLUDE_DOCS)
    INCLUDE(${ARCADIA_ROOT}/ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate/ya.make.inc)
ENDIF()

END()

RECURSE(
    docs_generate
)
