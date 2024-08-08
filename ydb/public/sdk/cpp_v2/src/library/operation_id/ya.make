LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    operation_id.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/uri
)

END()
