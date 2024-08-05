LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    discovery_mutator.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/extension_common
)

END()
