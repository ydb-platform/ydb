LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    kqp_session_common.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp_v2/src/library/operation_id
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_endpoints
)


END()
