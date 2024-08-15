LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    yql_issue.cpp
    yql_issue_id.cpp
    yql_issue_message.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/colorizer
    library/cpp/resource
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/library/string_utils/helpers
    ydb/public/sdk/cpp_v2/src/library/yql_common/issue/protos
    ydb/public/sdk/cpp_v2/src/library/yql_common/utils
)

END()
