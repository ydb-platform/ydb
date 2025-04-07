LIBRARY()

SRCS(
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/ydb_endpoints
    ydb/public/sdk/cpp/src/client/types/operation
)

END()
