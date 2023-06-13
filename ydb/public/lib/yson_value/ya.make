LIBRARY()

SRCS(
    ydb_yson_value.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_value
    ydb/library/uuid
)

END()
