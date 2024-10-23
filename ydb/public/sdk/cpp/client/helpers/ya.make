LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    helpers.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/helpers
)

END()
