LIBRARY()

SRCS(
    kqp_session_common.cpp
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/endpoints
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/public/sdk/cpp/src/library/operation_id
)

END()
