LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    make.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
)

END()
