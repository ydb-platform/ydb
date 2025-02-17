LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    handlers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_types/exceptions
)

END()
