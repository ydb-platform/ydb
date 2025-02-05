LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    params.cpp
    impl.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers
    ydb/public/sdk/cpp/client/ydb_value
)

END()
