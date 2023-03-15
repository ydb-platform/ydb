LIBRARY()

SRCS(
    client.cpp
    client.h
    query.cpp
    query.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/draft/ydb_query/impl
    ydb/public/sdk/cpp/client/ydb_common_client
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_types/operation
)

END()
