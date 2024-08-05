PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    issue_message.proto
    issue_severity.proto
)

EXCLUDE_TAGS(GO_PROTO)

END()
