LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    discovery_mutator.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/extension_common
)

END()
