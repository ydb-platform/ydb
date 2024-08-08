LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    retry.cpp
    utils.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/retry/protos
)

END()

RECURSE(
    protos
)
