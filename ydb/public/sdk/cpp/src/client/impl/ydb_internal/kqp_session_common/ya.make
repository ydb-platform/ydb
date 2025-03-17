LIBRARY()

SRCS(
    kqp_session_common.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/impl/ydb_endpoints
)


END()
