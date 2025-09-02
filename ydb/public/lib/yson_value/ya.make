LIBRARY()

SRCS(
    ydb_yson_value.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/value
    yql/essentials/types/uuid
)

END()
