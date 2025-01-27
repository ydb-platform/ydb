LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    operation_id.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/operation_id/protos
    library/cpp/cgiparam
    library/cpp/uri
)

END()
