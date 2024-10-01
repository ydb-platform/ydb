LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    discovery_mutator.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/extensions/discovery_mutator
)

END()
