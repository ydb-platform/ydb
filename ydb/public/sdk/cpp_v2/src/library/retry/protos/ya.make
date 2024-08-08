PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    retry_options.proto
)

PEERDIR()

EXCLUDE_TAGS(GO_PROTO)

END()
