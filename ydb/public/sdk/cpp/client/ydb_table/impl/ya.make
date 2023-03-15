LIBRARY()

SRCS(
    client_session.cpp
    data_query.cpp
    request_migrator.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/lib/operation_id/protos
    ydb/public/sdk/cpp/client/impl/ydb_endpoints
    ydb/library/yql/public/issue/protos
)

END()
