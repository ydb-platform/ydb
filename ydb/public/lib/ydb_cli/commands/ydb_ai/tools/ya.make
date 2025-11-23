LIBRARY()

SRCS(
    exec_query_tool.cpp
)

PEERDIR(
    library/cpp/json/writer
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/query
)

END()
