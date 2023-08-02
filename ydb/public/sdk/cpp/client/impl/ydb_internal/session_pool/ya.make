LIBRARY()

SRCS(
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/impl/ydb_endpoints
    ydb/public/sdk/cpp/client/ydb_types/operation
    ydb/library/yql/public/issue/protos
)

END()
