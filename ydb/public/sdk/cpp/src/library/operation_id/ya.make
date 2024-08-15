LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    operation_id.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/uri
)

END()
