LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_endpoints
    ydb/public/sdk/cpp_v2/src/client/types/operation
)

END()
