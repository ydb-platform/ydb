LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    make.cpp
)

PEERDIR(
    contrib/libs/protobuf
    ydb/public/api/protos
)

END()
