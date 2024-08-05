LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    operation_id.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/cgiparam
    library/cpp/uri
    ydb/public/sdk/cpp_v2/src/library/operation_id/protos
)

END()
