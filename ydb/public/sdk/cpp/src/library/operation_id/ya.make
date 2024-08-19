LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    operation_id.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/uri
)

END()
