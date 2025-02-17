LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    kqp_session_common.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/lib/operation_id
    ydb/public/sdk/cpp/client/impl/ydb_endpoints
)


END()
