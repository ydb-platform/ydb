LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    rate_limiter.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/rate_limiter
)

END()
