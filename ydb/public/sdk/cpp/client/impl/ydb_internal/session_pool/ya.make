LIBRARY()

SRCS(
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/impl/ydb_endpoints
    ydb/library/yql/public/issue/protos
)

END()
