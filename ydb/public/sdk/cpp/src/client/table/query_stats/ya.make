LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    stats.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/query
)

END()
