LIBRARY()

SRCS(
    kqp_session_common.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/lib/operation_id/protos
    ydb/public/sdk/cpp/client/impl/ydb_endpoints
)


END()
