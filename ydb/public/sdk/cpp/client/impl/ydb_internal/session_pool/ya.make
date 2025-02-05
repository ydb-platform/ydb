LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    session_pool.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/impl/ydb_endpoints
    ydb/public/sdk/cpp/client/ydb_types/operation
    yql/essentials/public/issue/protos
)

END()
