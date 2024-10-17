LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    import.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/import
)

END()
