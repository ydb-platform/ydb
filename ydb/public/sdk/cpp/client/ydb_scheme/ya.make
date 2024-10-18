LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    scheme.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/scheme
)

END()
