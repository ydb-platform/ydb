LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

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
    ydb/public/sdk/cpp/src/library/string_utils/helpers
    ydb/public/sdk/cpp/src/library/yql_common/issue/protos
    ydb/public/sdk/cpp/src/library/yql_common/utils
)

END()
