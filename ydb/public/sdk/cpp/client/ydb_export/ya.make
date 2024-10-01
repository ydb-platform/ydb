LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    export.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/export
)

END()
