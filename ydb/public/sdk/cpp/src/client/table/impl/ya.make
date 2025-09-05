LIBRARY()

SRCS(
    client_session.cpp
    data_query.cpp
    readers.cpp
    request_migrator.cpp
    table_client.cpp
    transaction.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/endpoints
    ydb/public/sdk/cpp/src/client/impl/session
    ydb/public/sdk/cpp/src/client/table/query_stats
    ydb/public/sdk/cpp/src/library/string_utils/helpers
)

END()
