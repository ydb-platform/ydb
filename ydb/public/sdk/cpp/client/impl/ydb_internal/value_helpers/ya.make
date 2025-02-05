LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers
)

END()
